package flink.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleHistogram;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.configuration.Configuration;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive metrics collector for Flink streaming jobs
 * that tracks detailed processing information and correlates with sender metrics.
 */
public class FlinkMetricsCollector {
    // Static constants
    private static final int HISTOGRAM_BUCKETS = 10;
    private static final int MAX_BATCH_HISTORY = 1000;
    
    // Singleton instance
    private static FlinkMetricsCollector INSTANCE;
    
    // Configuration
    private final String metricsFilePath;
    private final boolean detailedLogging;
    private final String jobId;
    private final String runId;
    
    // Metrics writers
    private PrintWriter summaryWriter;
    private PrintWriter detailWriter;
    private PrintWriter latencyWriter;
    
    // Metrics data structures
    private final AtomicLong recordsReceived = new AtomicLong(0);
    private final AtomicLong bytesReceived = new AtomicLong(0);
    private final AtomicLong recordsProcessed = new AtomicLong(0);
    private final AtomicLong resultsEmitted = new AtomicLong(0);
    
    // Batch tracking
    private final Map<String, BatchInfo> batchTracker = new ConcurrentHashMap<>();
    private final List<BatchInfo> recentBatches = Collections.synchronizedList(new ArrayList<>(MAX_BATCH_HISTORY));
    
    // Timing information
    private long firstRecordTimestamp = 0;
    private long lastRecordTimestamp = 0;
    private long processingStartTime = 0;
    private long processingEndTime = 0;
    
    // Latency tracking
    private final SimpleHistogram processingLatencyHistogram = new SimpleHistogram(HISTOGRAM_BUCKETS);
    private final SimpleHistogram endToEndLatencyHistogram = new SimpleHistogram(HISTOGRAM_BUCKETS);
    
    // Throughput tracking
    private final MeterRegistry meterRegistry = new MeterRegistry();
    
    /**
     * Internal class to track batch information
     */
    private static class BatchInfo {
        final String batchUuid;
        final String sourceId;
        final long sequenceNumber;
        final long creationTimestamp;
        long firstRecordReceived;
        long lastRecordReceived;
        long recordCount;
        long byteCount;
        boolean completed;
        
        public BatchInfo(String batchUuid, String sourceId, long sequenceNumber, long creationTimestamp) {
            this.batchUuid = batchUuid;
            this.sourceId = sourceId;
            this.sequenceNumber = sequenceNumber;
            this.creationTimestamp = creationTimestamp;
            this.firstRecordReceived = 0;
            this.lastRecordReceived = 0;
            this.recordCount = 0;
            this.byteCount = 0;
            this.completed = false;
        }
        
        public void recordReceived(long timestamp, long bytes) {
            if (firstRecordReceived == 0) {
                firstRecordReceived = timestamp;
            }
            lastRecordReceived = timestamp;
            recordCount++;
            byteCount += bytes;
        }
        
        public void markCompleted() {
            this.completed = true;
        }
        
        public double getProcessingTimeMs() {
            if (firstRecordReceived == 0 || lastRecordReceived == 0) {
                return 0;
            }
            return (lastRecordReceived - firstRecordReceived) / 1_000_000.0;
        }
        
        public double getEndToEndLatencyMs() {
            if (firstRecordReceived == 0 || creationTimestamp == 0) {
                return 0;
            }
            return (firstRecordReceived - creationTimestamp) / 1_000_000.0;
        }
    }
    
    /**
     * Simple meter registry for tracking throughput
     */
    private static class MeterRegistry {
        private final Map<String, MeterView> meters = new HashMap<>();
        
        public MeterView getMeter(String name) {
            return meters.computeIfAbsent(name, k -> new MeterView(60));
        }
        
        public void markEvent(String name, long value) {
            getMeter(name).markEvent(value);
        }
        
        public double getRate(String name) {
            MeterView meter = meters.get(name);
            return meter != null ? meter.getRate() : 0;
        }
    }
    
    /**
     * Get the singleton instance of the FlinkMetricsCollector
     */
    public static synchronized FlinkMetricsCollector getInstance(String metricsFilePath, String jobId) {
        if (INSTANCE == null) {
            INSTANCE = new FlinkMetricsCollector(metricsFilePath, true, jobId);
        }
        return INSTANCE;
    }
    
    /**
     * Private constructor
     */
    private FlinkMetricsCollector(String metricsFilePath, boolean detailedLogging, String jobId) {
        this.metricsFilePath = metricsFilePath;
        this.detailedLogging = detailedLogging;
        this.jobId = jobId;
        this.runId = UUID.randomUUID().toString();
        
        initializeMetricsFiles();
        
        // Record processing start time
        this.processingStartTime = System.nanoTime();
    }
    
    /**
     * Initialize metrics files with headers
     */
    private void initializeMetricsFiles() {
        try {
            // Create directory if it doesn't exist
            File directory = new File(metricsFilePath).getParentFile();
            if (directory != null && !directory.exists()) {
                directory.mkdirs();
            }
            
            // Create base filename with timestamp
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String baseFilename = metricsFilePath.replace(".csv", "");
            
            // Create summary metrics file
            File summaryFile = new File(baseFilename + "_summary_" + timestamp + ".csv");
            summaryWriter = new PrintWriter(new FileWriter(summaryFile));
            summaryWriter.println("timestamp,run_id,job_id,operation,records_received,bytes_received," +
                                 "records_processed,results_emitted,processing_time_ms," +
                                 "throughput_records_per_sec,throughput_mb_per_sec," +
                                 "avg_processing_latency_ms,avg_end_to_end_latency_ms");
            summaryWriter.flush();
            
            // Create detailed metrics file if detailed logging is enabled
            if (detailedLogging) {
                File detailFile = new File(baseFilename + "_detail_" + timestamp + ".csv");
                detailWriter = new PrintWriter(new FileWriter(detailFile));
                detailWriter.println("timestamp,run_id,job_id,operation,batch_uuid,source_id," +
                                    "sequence_number,first_record_time,last_record_time," +
                                    "record_count,byte_count,processing_time_ms,end_to_end_latency_ms");
                detailWriter.flush();
                
                File latencyFile = new File(baseFilename + "_latency_" + timestamp + ".csv");
                latencyWriter = new PrintWriter(new FileWriter(latencyFile));
                latencyWriter.println("timestamp,run_id,job_id,operation,record_id," +
                                     "creation_time,receive_time,process_time,end_time," +
                                     "source_to_receive_ms,receive_to_process_ms,process_to_end_ms,total_latency_ms");
                latencyWriter.flush();
            }
            
            // Log initialization entry
            logSummaryEntry("INITIALIZED");
        } catch (IOException e) {
            System.err.println("Failed to initialize metrics files: " + e.getMessage());
        }
    }
    
    /**
     * Log a summary entry
     */
    private void logSummaryEntry(String operation) {
        if (summaryWriter == null) return;
        
        long now = System.currentTimeMillis();
        long processingTimePassed = (System.nanoTime() - processingStartTime) / 1_000_000;
        
        double recordsPerSec = processingTimePassed > 0 ? 
            (recordsReceived.get() * 1000.0 / processingTimePassed) : 0;
        
        double mbPerSec = processingTimePassed > 0 ? 
            (bytesReceived.get() / (1024.0 * 1024.0) * 1000.0 / processingTimePassed) : 0;
        
        synchronized (summaryWriter) {
            summaryWriter.printf("%d,%s,%s,%s,%d,%d,%d,%d,%d,%.6f,%.6f,%.6f,%.6f\n",
                now, runId, jobId, operation,
                recordsReceived.get(), bytesReceived.get(),
                recordsProcessed.get(), resultsEmitted.get(),
                processingTimePassed,
                recordsPerSec, mbPerSec,
                processingLatencyHistogram.getCount() > 0 ? processingLatencyHistogram.getMean() : 0,
                endToEndLatencyHistogram.getCount() > 0 ? endToEndLatencyHistogram.getMean() : 0);
            summaryWriter.flush();
        }
    }
    
    /**
     * Log a batch completion
     */
    private void logBatchCompletion(BatchInfo batch) {
        if (detailWriter == null || !detailedLogging) return;
        
        synchronized (detailWriter) {
            detailWriter.printf("%d,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%.6f,%.6f\n",
                System.currentTimeMillis(), runId, jobId, "BATCH_COMPLETED",
                batch.batchUuid, batch.sourceId, batch.sequenceNumber,
                batch.firstRecordReceived, batch.lastRecordReceived,
                batch.recordCount, batch.byteCount,
                batch.getProcessingTimeMs(), batch.getEndToEndLatencyMs());
            detailWriter.flush();
        }
    }
    
    /**
     * Log record latency information
     */
    private void logRecordLatency(String recordId, long creationTime, long receiveTime, 
                                 long processTime, long endTime) {
        if (latencyWriter == null || !detailedLogging) return;
        
        double sourceToReceiveMs = (receiveTime - creationTime) / 1_000_000.0;
        double receiveToProcessMs = (processTime - receiveTime) / 1_000_000.0;
        double processToEndMs = (endTime - processTime) / 1_000_000.0;
        double totalLatencyMs = (endTime - creationTime) / 1_000_000.0;
        
        synchronized (latencyWriter) {
            latencyWriter.printf("%d,%s,%s,%s,%s,%d,%d,%d,%d,%.6f,%.6f,%.6f,%.6f\n",
                System.currentTimeMillis(), runId, jobId, "RECORD_LATENCY",
                recordId, creationTime, receiveTime, processTime, endTime,
                sourceToReceiveMs, receiveToProcessMs, processToEndMs, totalLatencyMs);
            latencyWriter.flush();
        }
    }
    
    /**
     * Record a batch from the incoming data
     */
    public void recordBatch(String batchUuid, String sourceId, long sequenceNumber, 
                           long creationTimestamp) {
        BatchInfo batch = new BatchInfo(batchUuid, sourceId, sequenceNumber, creationTimestamp);
        batchTracker.put(batchUuid, batch);
        
        // Keep track of recent batches (limited history)
        recentBatches.add(batch);
        if (recentBatches.size() > MAX_BATCH_HISTORY) {
            recentBatches.remove(0);
        }
    }
    
    /**
     * Record a received record
     */
    public void recordReceived(String batchUuid, String recordId, long timestamp, long bytes, 
                              long creationTimestamp) {
        // Update global counters
        recordsReceived.incrementAndGet();
        bytesReceived.addAndGet(bytes);
        
        // Update batch information if available
        BatchInfo batch = batchTracker.get(batchUuid);
        if (batch != null) {
            batch.recordReceived(timestamp, bytes);
        }
        
        // Update first and last record timestamps
        if (firstRecordTimestamp == 0) {
            firstRecordTimestamp = timestamp;
        }
        lastRecordTimestamp = timestamp;
        
        // Update throughput metering
        meterRegistry.markEvent("records_received", 1);
        meterRegistry.markEvent("bytes_received", bytes);
        
        // Calculate and record end-to-end latency if creation timestamp is available
        if (creationTimestamp > 0) {
            double latencyMs = (timestamp - creationTimestamp) / 1_000_000.0;
            endToEndLatencyHistogram.update((long) latencyMs);
        }
        
        // Log metrics periodically
        if (recordsReceived.get() % 10000 == 0) {
            logSummaryEntry("PROGRESS");
        }
    }
    
    /**
     * Record a processed record
     */
    public void recordProcessed(String recordId, long processStartTime, long processEndTime) {
        recordsProcessed.incrementAndGet();
        
        // Calculate and record processing latency
        double latencyMs = (processEndTime - processStartTime) / 1_000_000.0;
        processingLatencyHistogram.update((long) latencyMs);
        
        // Update throughput metering
        meterRegistry.markEvent("records_processed", 1);
        
        // Log metrics periodically
        if (recordsProcessed.get() % 10000 == 0) {
            logSummaryEntry("PROCESSING_PROGRESS");
        }
    }
    
    /**
     * Record a completed batch
     */
    public void recordBatchCompleted(String batchUuid) {
        BatchInfo batch = batchTracker.get(batchUuid);
        if (batch != null) {
            batch.markCompleted();
            logBatchCompletion(batch);
        }
    }
    
    /**
     * Record a result emitted
     */
    public void recordResultEmitted() {
        resultsEmitted.incrementAndGet();
        meterRegistry.markEvent("results_emitted", 1);
    }
    
    /**
     * Finalize metrics when the job completes
     */
    public void finalize() {
        // Record processing end time
        processingEndTime = System.nanoTime();
        
        // Log summary with final statistics
        logSummaryEntry("COMPLETED");
        
        // Close all writers
        if (summaryWriter != null) {
            summaryWriter.close();
        }
        
        if (detailWriter != null) {
            detailWriter.close();
        }
        
        if (latencyWriter != null) {
            latencyWriter.close();
        }
    }
    
    /**
     * Get total processing time in milliseconds
     */
    public double getTotalProcessingTimeMs() {
        long endTime = processingEndTime > 0 ? processingEndTime : System.nanoTime();
        return (endTime - processingStartTime) / 1_000_000.0;
    }
    
    /**
     * Get current throughput in records per second
     */
    public double getRecordThroughput() {
        return meterRegistry.getRate("records_received");
    }
    
    /**
     * Get current throughput in MB per second
     */
    public double getMBThroughput() {
        return meterRegistry.getRate("bytes_received") / (1024.0 * 1024.0);
    }
    
    /**
     * Get average processing latency in milliseconds
     */
    public double getAvgProcessingLatencyMs() {
        return processingLatencyHistogram.getCount() > 0 ? 
            processingLatencyHistogram.getMean() : 0;
    }
    
    /**
     * Get average end-to-end latency in milliseconds
     */
    public double getAvgEndToEndLatencyMs() {
        return endToEndLatencyHistogram.getCount() > 0 ? 
            endToEndLatencyHistogram.getMean() : 0;
    }
}