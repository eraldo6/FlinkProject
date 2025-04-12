import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import flink.source.EnhancedTcpSource;
import flink.metrics.FlinkMetricsCollector;

import java.io.File;
import java.util.UUID;

/**
 * Enhanced version of the SmartNIC streaming benchmark with comprehensive metrics
 */
public class EnhancedSmartNICStreamingBenchmark {

    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        String metricsDir = "/path/to/metrics";
        int tcpPort = 8000;
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--metrics-dir") && i + 1 < args.length) {
                metricsDir = args[i + 1];
                i++;
            } else if (args[i].equals("--port") && i + 1 < args.length) {
                tcpPort = Integer.parseInt(args[i + 1]);
                i++;
            }
        }
        
        // Create metrics directory if it doesn't exist
        File metricsDirFile = new File(metricsDir);
        if (!metricsDirFile.exists()) {
            metricsDirFile.mkdirs();
        }
        
        // Create metrics file paths
        String metricsFilePath = metricsDir + "/flink_metrics.csv";
        
        // Set up Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.disableOperatorChaining();
        
        // Create a unique job ID for this run
        final String jobId = UUID.randomUUID().toString();

        // Set up table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);          

        // Set up enhanced TCP source with metrics
        DataStream<String> sourceStream = env.addSource(
            new EnhancedTcpSource(tcpPort, metricsFilePath, true))
            .name("Enhanced-TCP-Source")
            .uid("source-" + UUID.randomUUID().toString());

        // Add processing timestamps to track latency
        DataStream<String> timestampedStream = sourceStream
            .map(new RichMapFunction<String, String>() {
                private transient FlinkMetricsCollector metricsCollector;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    String jobId = getRuntimeContext().getJobId().toString();
                    metricsCollector = FlinkMetricsCollector.getInstance(metricsFilePath, jobId);
                }
                
                @Override
                public String map(String value) throws Exception {
                    // Record that this record was processed
                    long processTime = System.nanoTime();
                    
                    // Here we would normally do some actual processing
                    
                    // Record processing in metrics
                    metricsCollector.recordProcessed("record-" + UUID.randomUUID().toString(), 
                                                    processTime, System.nanoTime());
                    
                    return value;
                }
            })
            .name("ProcessingTimestamper")
            .uid("timestamper-" + UUID.randomUUID().toString());

        // Register tables from the streaming data
        registerTablesFromStream(tableEnv, timestampedStream);
        
        // Execute the query
        Table resultTable = executeQuery(tableEnv);
        
        // Convert result back to DataStream
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
        
        // Add a metrics-aware sink to track result emission
        resultStream.addSink(new RichSinkFunction<Row>() {
            private transient FlinkMetricsCollector metricsCollector;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                String jobId = getRuntimeContext().getJobId().toString();
                metricsCollector = FlinkMetricsCollector.getInstance(metricsFilePath, jobId);
            }
            
            @Override
            public void invoke(Row value, Context context) throws Exception {
                // Record result emission
                metricsCollector.recordResultEmitted();
                
                // In a real application, would write results somewhere
                System.out.println("Result: " + value);
            }
        })
        .name("MetricsAwareSink")
        .uid("sink-" + UUID.randomUUID().toString());
        
        // Execute the job
        env.execute("Enhanced-SmartNIC-Streaming-Benchmark-" + jobId);
    }

    private static void registerTablesFromStream(StreamTableEnvironment tableEnv, DataStream<String> sourceStream) {
        // For store_sales: We expect data with a prefix like "store_sales|"
        DataStream<Row> storeSalesStream = sourceStream
            .filter(line -> line.toLowerCase().startsWith("store_sales|"))
            .map(line -> {
                String[] parts = line.substring(line.indexOf('|') + 1).split("\\|", -1);
                Row row = Row.withNames();
                
                // Set fields needed for the query
                if (parts.length >= 16) {
                    row.setField("ss_sold_date_sk", parts[0].isEmpty() ? null : Long.parseLong(parts[0]));
                    row.setField("ss_item_sk", parts[2].isEmpty() ? null : Long.parseLong(parts[2]));
                    row.setField("ss_quantity", parts[10].isEmpty() ? null : Integer.parseInt(parts[10]));
                    row.setField("ss_ext_sales_price", parts[15].isEmpty() ? null : Double.parseDouble(parts[15]));
                }
                
                return row;
            }).returns(Types.ROW_NAMED(
                new String[] {"ss_sold_date_sk", "ss_item_sk", "ss_quantity", "ss_ext_sales_price"},
                Types.LONG, Types.LONG, Types.INT, Types.DOUBLE
            ));
        
        Table storeSalesTable = tableEnv.fromDataStream(storeSalesStream);
        tableEnv.createTemporaryView("store_sales", storeSalesTable);
        
        // For date_dim: We expect data with a prefix like "date_dim|"
        DataStream<Row> dateDimStream = sourceStream
            .filter(line -> line.toLowerCase().startsWith("date_dim|"))
            .map(line -> {
                String[] parts = line.substring(line.indexOf('|') + 1).split("\\|", -1);
                Row row = Row.withNames();
                
                // Set fields needed for the query
                if (parts.length >= 9) {
                    row.setField("d_date_sk", parts[0].isEmpty() ? null : Long.parseLong(parts[0]));
                    row.setField("d_year", parts[6].isEmpty() ? null : Integer.parseInt(parts[6]));
                    // Add d_moy (month of year) used in the filter
                    row.setField("d_moy", parts[8].isEmpty() ? null : Integer.parseInt(parts[8]));
                }
                
                return row;
            }).returns(Types.ROW_NAMED(
                new String[] {"d_date_sk", "d_year", "d_moy"},
                Types.LONG, Types.INT, Types.INT
            ));
        
        Table dateDimTable = tableEnv.fromDataStream(dateDimStream);
        tableEnv.createTemporaryView("date_dim", dateDimTable);
        
        // For item: We expect data with a prefix like "item|"
        DataStream<Row> itemStream = sourceStream
            .filter(line -> line.toLowerCase().startsWith("item|"))
            .map(line -> {
                String[] parts = line.substring(line.indexOf('|') + 1).split("\\|", -1);
                Row row = Row.withNames();
                
                // Set fields needed for the query
                if (parts.length >= 14) {
                    row.setField("i_item_sk", parts[0].isEmpty() ? null : Long.parseLong(parts[0]));
                    row.setField("i_brand_id", parts[7].isEmpty() ? null : Integer.parseInt(parts[7]));
                    row.setField("i_brand", parts[8]);
                    // Add i_manufact_id used in the filter
                    row.setField("i_manufact_id", parts[13].isEmpty() ? null : Integer.parseInt(parts[13]));
                }
                
                return row;
            }).returns(Types.ROW_NAMED(
                new String[] {"i_item_sk", "i_brand_id", "i_brand", "i_manufact_id"},
                Types.LONG, Types.INT, Types.STRING, Types.INT
            ));
        
        Table itemTable = tableEnv.fromDataStream(itemStream);
        tableEnv.createTemporaryView("item", itemTable);
    }

    private static Table executeQuery(StreamTableEnvironment tableEnv) {
        String query = "SELECT " +
                       "dt.d_year, " +
                       "item.i_brand_id AS brand_id, " +
                       "item.i_brand AS brand, " +
                       "store_sales.ss_quantity, " +
                       "item.i_manufact_id, " +
                       "dt.d_moy, " +
                       "SUM(ss_ext_sales_price) AS sum_agg " +
                       "FROM store_sales " +
                       "JOIN date_dim dt ON dt.d_date_sk = store_sales.ss_sold_date_sk " +
                       "JOIN item ON store_sales.ss_item_sk = item.i_item_sk " +
                       "WHERE item.i_manufact_id = 436 AND dt.d_moy = 12 AND store_sales.ss_quantity < 10 " +
                       "GROUP BY dt.d_year, item.i_brand, item.i_brand_id, store_sales.ss_quantity, item.i_manufact_id, dt.d_moy";
        
        return tableEnv.sqlQuery(query);
    }
}