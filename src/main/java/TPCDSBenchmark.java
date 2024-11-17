import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TPCDSBenchmark {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String dataPath = "/home/egrabovaj/tpcds-kit/dataset";

        registerStoreReturns(tableEnv, dataPath);
        registerDateDim(tableEnv, dataPath);
        registerStore(tableEnv, dataPath);
        registerCustomer(tableEnv, dataPath);

        executeQuery(tableEnv);
    }

    private static void registerStoreReturns(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE store_returns (" +
                        "sr_return_time_sk BIGINT, " +
                        "sr_item_sk BIGINT, " +
                        "sr_customer_sk BIGINT, " +
                        "sr_cdemo_sk BIGINT, " +
                        "sr_hdemo_sk BIGINT, " +
                        "sr_addr_sk BIGINT, " +
                        "sr_store_sk BIGINT, " +
                        "sr_reason_sk BIGINT, " +
                        "sr_ticket_number BIGINT, " +
                        "sr_return_quantity INT, " +
                        "sr_return_amt DOUBLE, " +
                        "sr_return_tax DOUBLE, " +
                        "sr_return_amt_inc_tax DOUBLE, " +
                        "sr_fee DOUBLE, " +
                        "sr_return_ship_cost DOUBLE, " +
                        "sr_refunded_cash DOUBLE, " +
                        "sr_reversed_charge DOUBLE, " +
                        "sr_store_credit DOUBLE, " +
                        "sr_net_loss DOUBLE, " +
                        "sr_returned_date_sk BIGINT" +
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/store_returns.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " +   // Ensure this matches your actual data file delimiter
                        "'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );
    }

    private static void registerDateDim(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE date_dim (" +
                        "d_date_sk BIGINT, " +
                        "d_date_id STRING, " +
                        "d_date DATE, " +
                        "d_month_seq INT, " +
                        "d_week_seq INT, " +
                        "d_quarter_seq INT, " +
                        "d_year INT, " +
                        "d_dow INT, " +
                        "d_moy INT, " +
                        "d_dom INT, " +
                        "d_qoy INT, " +
                        "d_fy_year INT, " +
                        "d_fy_quarter_seq INT, " +
                        "d_fy_week_seq INT, " +
                        "d_day_name STRING, " +
                        "d_quarter_name STRING, " +
                        "d_holiday STRING, " +
                        "d_weekend STRING, " +
                        "d_following_holiday STRING, " +
                        "d_first_dom INT, " +
                        "d_last_dom INT, " +
                        "d_same_day_ly INT, " +
                        "d_same_day_lq INT, " +
                        "d_current_day STRING, " +
                        "d_current_week STRING, " +
                        "d_current_month STRING, " +
                        "d_current_quarter STRING, " +
                        "d_current_year STRING" +
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/date_dim.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " +
                        "'csv.ignore-parse-errors' = 'true', " +
                        "'csv.null-literal' = 'null'" +  // Handling NULL values
                        ")"
        );
    }

    private static void registerStore(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE store (" +
                        "s_store_sk BIGINT, " +
                        "s_store_id STRING, " +
                        "s_rec_start_date DATE, " +
                        "s_rec_end_date DATE, " +
                        "s_closed_date_sk BIGINT, " +
                        "s_store_name STRING, " +
                        "s_number_employees INT, " +
                        "s_floor_space INT, " +
                        "s_hours STRING, " +
                        "s_manager STRING, " +
                        "s_market_id INT, " +
                        "s_geography_class STRING, " +
                        "s_market_desc STRING, " +
                        "s_market_manager STRING, " +
                        "s_division_id INT, " +
                        "s_division_name STRING, " +
                        "s_company_id INT, " +
                        "s_company_name STRING, " +
                        "s_street_number STRING, " +
                        "s_street_name STRING, " +
                        "s_street_type STRING, " +
                        "s_suite_number STRING, " +
                        "s_city STRING, " +
                        "s_county STRING, " +
                        "s_state STRING, " +
                        "s_zip STRING, " +
                        "s_country STRING, " +
                        "s_gmt_offset DOUBLE, " +
                        "s_tax_percentage DOUBLE" +
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/store.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " + // Ensure your data uses this delimiter
                        "'csv.ignore-parse-errors' = 'true', " + // Continue processing if minor errors found
                        "'csv.null-literal' = 'null'" + // Handle null values appropriately
                        ")"
        );
    }

    private static void registerCustomer(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE customer (" +
                        "c_customer_sk BIGINT, " +
                        "c_customer_id STRING, " +
                        "c_current_cdemo_sk BIGINT, " +
                        "c_current_hdemo_sk BIGINT, " +
                        "c_current_addr_sk BIGINT, " +
                        "c_first_shipto_date_sk BIGINT, " +
                        "c_first_sales_date_sk BIGINT, " +
                        "c_salutation STRING, " +
                        "c_first_name STRING, " +
                        "c_last_name STRING, " +
                        "c_preferred_cust_flag STRING, " +
                        "c_birth_day INT, " +
                        "c_birth_month INT, " +
                        "c_birth_year INT, " +
                        "c_birth_country STRING, " +
                        "c_login STRING, " +
                        "c_email_address STRING, " +
                        "c_last_review_date_sk STRING" + // Assuming this should be STRING if it's a formatted date; change if it's a SK reference
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/customer.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " + // Ensure your data uses this delimiter
                        "'csv.ignore-parse-errors' = 'true', " + // Continue processing if minor errors found
                        "'csv.null-literal' = 'null'" + // Handle null values appropriately
                        ")"
        );
    }

    private static void executeQuery(TableEnvironment tableEnv) {
//        String query = "WITH customer_total_return AS (" +
//                "SELECT sr_customer_sk AS ctr_customer_sk, " +
//                "       sr_store_sk AS ctr_store_sk, " +
//                "       SUM(sr_fee) AS ctr_total_return " +
//                "FROM store_returns, date_dim " +
//                "WHERE sr_returned_date_sk = d_date_sk " +
//                "  AND d_year = 2000 " +
//                "GROUP BY sr_customer_sk, sr_store_sk" +
//                ") " +
//                "SELECT c.c_customer_id " +
//                "FROM customer_total_return ctr1 " +
//                "JOIN store s ON s.s_store_sk = ctr1.ctr_store_sk " +
//                "JOIN customer c ON c.c_customer_sk = ctr1.ctr_customer_sk " +
//                "WHERE ctr1.ctr_total_return > ( " +
//                "    SELECT AVG(ctr_total_return) * 1.2 " +
//                "    FROM customer_total_return ctr2 " +
//                "    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk" +
//                ") " +
//                "AND s.s_state = 'NM' " +
//                "ORDER BY c.c_customer_id " +
//                "LIMIT 100";
//
//
//
//        tableEnv.executeSql(query).print();

        // Test if tables are loaded correctly
        tableEnv.executeSql("SELECT COUNT(*) FROM store_returns").print();
        tableEnv.executeSql("SELECT COUNT(*) FROM date_dim WHERE d_year = 2000").print();
        tableEnv.executeSql("SELECT COUNT(*) FROM store").print();
        tableEnv.executeSql("SELECT COUNT(*) FROM customer").print();

        // Test a simplified version of the query
        String simpleQuery = "SELECT sr_customer_sk, SUM(sr_fee) AS total_return " +
                "FROM store_returns " +
                "GROUP BY sr_customer_sk " +
                "HAVING SUM(sr_fee) > 1000"; // Adjust based on your data scale
        tableEnv.executeSql(simpleQuery).print();
    }
}
