import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import flink.source.NewSource;

public class SmartNICStreamingBenchmark {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Switch to STREAMING mode
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env.disableOperatorChaining();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);          

        // Set up TCP source to listen for data
        int port = 8000;
        DataStream<String> sourceStream = env.addSource(
            new NewSource(port, "/path/to/metrics/flink_source_metrics.csv", true))
            .name("TCP-Source");

        // Register tables from streaming data
        registerTablesFromStream(tableEnv, sourceStream);
        
        // Execute the query
        executeQuery(tableEnv);
        
        // Execute the job
        env.execute("TPCDSBenchmark-Streaming");
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

    private static void executeQuery(StreamTableEnvironment tableEnv) {
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
        
        tableEnv.executeSql(query).print();
    }
}