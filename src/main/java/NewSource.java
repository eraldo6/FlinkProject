package flink.source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Custom Flink source that reads data from a TCP socket.
 * Enhanced with detailed logging, performance metrics, and EOT detection.
 */
public class NewSource implements SourceFunction<String> {

    private final int port;
    private volatile boolean isRunning = true;
    private ServerSocket serverSocket;
    
    // Performance metrics
    private final AtomicLong receivedRecords = new AtomicLong(0);
    private final AtomicLong receivedBytes = new AtomicLong(0);
    private final AtomicLong transferCount = new AtomicLong(0);
    private final long LOG_INTERVAL = 1000; // Log every 1000 records
    
    // End-of-Transmission marker
    private static final String EOT_MARKER = "<<<EOT_MARKER>>>";
    
    // Metrics file
    private PrintWriter metricsWriter = null;
    private final String metricsFilePath;
    private boolean enableFileMetrics;
    
    // Timing metrics
    private long transferStartTime = 0;
    private long lastLogTime = 0;
    private long lastBytesReceived = 0;
    private long lastRecordsReceived = 0;
    
    /**
     * Constructor with default metrics file location
     */
    public NewSource(int port) {
        this(port, "flink_source_metrics.csv", false);
    }
    
    /**
     * Constructor with custom metrics file location
     */
    public NewSource(int port, String metricsFilePath, boolean enableFileMetrics) {
        this.port = port;
        this.metricsFilePath = metricsFilePath;
        this.enableFileMetrics = enableFileMetrics;
    }
    
    /**
     * Initializes metrics file with headers
     */
    private void initMetricsFile() {
        if (!enableFileMetrics) return;
        
        try {
            // Create timestamped filename
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timestamp = dateFormat.format(new Date());
            String fullPath = metricsFilePath.replace(".csv", "_" + timestamp + ".csv");
            
            // Create metrics file
            metricsWriter = new PrintWriter(new FileWriter(fullPath));
            
            // Write CSV header
            metricsWriter.println("timestamp,event,records,bytes,duration_ms,records_per_sec,mb_per_sec,client_ip");
            metricsWriter.flush();
            
            System.out.println("[INFO] TCP Socket Source: Metrics file initialized: " + fullPath);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to initialize metrics file: " + e.getMessage());
            enableFileMetrics = false;
        }
    }
    
    /**
     * Logs metrics to both console and file
     */
    private void logMetrics(String event, long records, long bytes, long durationMs, String clientIp) {
        // Calculate rates
        double recordsPerSec = durationMs > 0 ? (records * 1000.0 / durationMs) : 0;
        double mbPerSec = durationMs > 0 ? (bytes / 1024.0 / 1024.0 * 1000.0 / durationMs) : 0;
        
        // Log to console
        System.out.println(String.format(
            "[METRICS] %s: %,d records, %,d bytes in %,d ms (%.2f records/sec, %.2f MB/sec)",
            event, records, bytes, durationMs, recordsPerSec, mbPerSec
        ));
        
        // Log to file if enabled
        if (enableFileMetrics && metricsWriter != null) {
            metricsWriter.println(String.format(
                "%d,%s,%d,%d,%d,%.2f,%.2f,%s",
                System.currentTimeMillis(), event, records, bytes, 
                durationMs, recordsPerSec, mbPerSec, clientIp
            ));
            metricsWriter.flush();
        }
    }
    
    /**
     * Logs interval metrics during transfer
     */
    private void logIntervalMetrics(String clientIp) {
        long currentTime = System.currentTimeMillis();
        long intervalMs = currentTime - lastLogTime;
        
        // Only log if it's been at least 5 seconds since last log
        if (intervalMs >= 5000) {
            long currentRecords = receivedRecords.get();
            long currentBytes = receivedBytes.get();
            
            long intervalRecords = currentRecords - lastRecordsReceived;
            long intervalBytes = currentBytes - lastBytesReceived;
            
            logMetrics("INTERVAL", intervalRecords, intervalBytes, intervalMs, clientIp);
            
            lastLogTime = currentTime;
            lastRecordsReceived = currentRecords;
            lastBytesReceived = currentBytes;
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // Initialize metrics file
        initMetricsFile();
        
        // Create a server socket that listens for connections
        serverSocket = new ServerSocket(port);
        System.out.println("[INFO] TCP Socket Source: Listening for connections on port " + port);
        
        while (isRunning) {
            Socket clientSocket = null;
            try {
                // Wait for client connection
                clientSocket = serverSocket.accept();
                String clientIp = clientSocket.getInetAddress().getHostAddress();
                System.out.println("[INFO] TCP Socket Source: Client connected from " + clientIp);
                
                // Reset transfer metrics
                long transferRecords = 0;
                long transferBytes = 0;
                transferStartTime = System.currentTimeMillis();
                lastLogTime = transferStartTime;
                lastRecordsReceived = receivedRecords.get();
                lastBytesReceived = receivedBytes.get();
                boolean transferCompleted = false;
                
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null && isRunning) {
                    if (line.trim().isEmpty()) {
                        continue; // Skip empty lines
                    }
                    
                    // Check for EOT marker
                    if (line.equals(EOT_MARKER)) {
                        long transferEndTime = System.currentTimeMillis();
                        long transferDuration = transferEndTime - transferStartTime;
                        
                        System.out.println("[INFO] TCP Socket Source: Received EOT marker, file transmission complete");
                        logMetrics("COMPLETE", transferRecords, transferBytes, transferDuration, clientIp);
                        
                        transferCount.incrementAndGet();
                        transferCompleted = true;
                        continue; // Don't process the marker as data
                    }
                    
                    // Count statistics
                    transferRecords++;
                    transferBytes += line.length();
                    long totalRecords = receivedRecords.incrementAndGet();
                    receivedBytes.addAndGet(line.length());
                    
                    // Log progress periodically
                    if (totalRecords % LOG_INTERVAL == 0) {
                        System.out.println("[INFO] TCP Socket Source: Received " + totalRecords + 
                                           " records, " + receivedBytes.get() + " bytes so far");
                        logIntervalMetrics(clientIp);
                    }
                    
                    // Log the actual data sample (first 100 chars if very long)
                    if (totalRecords % (LOG_INTERVAL * 10) == 0) {
                        if (line.length() > 100) {
                            System.out.println("[DEBUG] TCP Source received: " + 
                                            line.substring(0, 100) + "... (" + line.length() + " chars)");
                        } else {
                            System.out.println("[DEBUG] TCP Source received: " + line);
                        }
                    }
                    
                    // Output the data
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(line);
                    }
                }
                
                // If client disconnected without sending EOT marker
                if (!transferCompleted && transferRecords > 0) {
                    long transferEndTime = System.currentTimeMillis();
                    long transferDuration = transferEndTime - transferStartTime;
                    
                    System.out.println("[INFO] TCP Socket Source: Client disconnected without EOT marker");
                    logMetrics("INCOMPLETE", transferRecords, transferBytes, transferDuration, clientIp);
                }
                
                System.out.println("[INFO] TCP Socket Source: Client disconnected");
                
            } catch (Exception e) {
                System.err.println("[ERROR] TCP Socket Source error: " + e.getMessage());
                if (!isRunning) {
                    break; // Exit if shutting down
                }
                // Otherwise continue and wait for a new connection
            } finally {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }
            }
        }
        
        // Clean up resources
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        
        if (metricsWriter != null) {
            metricsWriter.close();
        }
        
        System.out.println("[INFO] TCP Socket Source shutdown complete");
        System.out.println("[SUMMARY] Received a total of " + receivedRecords.get() + 
                          " records, " + receivedBytes.get() + " bytes, " + 
                          transferCount.get() + " complete transfers");
    }

    @Override
    public void cancel() {
        System.out.println("[INFO] TCP Socket Source: Stopping...");
        isRunning = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (Exception e) {
            System.err.println("[ERROR] Error closing server socket: " + e.getMessage());
        }
    }
}