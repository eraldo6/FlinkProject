import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TPCDSBenchmark {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String dataPath = "/Users/eraldograbovaj/IdeaProjects/FlinkTPCDS/tpcds-kit/dataset";

        registerStoreReturns(tableEnv, dataPath);
        registerDateDim(tableEnv, dataPath);
        registerStore(tableEnv, dataPath);
        registerCustomer(tableEnv, dataPath);

        executeQuery(tableEnv);
    }

    private static void registerStoreReturns(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE store_returns (" +
                        "sr_customer_sk BIGINT, " +
                        "sr_store_sk BIGINT, " +
                        "sr_fee DOUBLE, " +
                        "sr_returned_date_sk BIGINT" +
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/store_returns.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " +   // Added field delimiter for .dat files
                        "'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );
    }

    private static void registerDateDim(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE date_dim (" +
                        "d_date_sk BIGINT, " +
                        "d_year INT" +
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/date_dim.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " +   // Added field delimiter
                        "'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );
    }

    private static void registerStore(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE store (" +
                        "s_store_sk BIGINT, " +
                        "s_state STRING" +
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/store.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " +   // Added field delimiter
                        "'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );
    }

    private static void registerCustomer(TableEnvironment tableEnv, String dataPath) {
        tableEnv.executeSql(
                "CREATE TABLE customer (" +
                        "c_customer_sk BIGINT, " +
                        "c_customer_id STRING" +
                        ") WITH (" +
                        "'connector' = 'filesystem', " +
                        "'path' = '" + dataPath + "/customer.dat', " +
                        "'format' = 'csv', " +
                        "'csv.field-delimiter' = '|', " +   // Added field delimiter
                        "'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );
    }

    private static void executeQuery(TableEnvironment tableEnv) {
        String query = "WITH customer_total_return AS (" +
                "SELECT sr_customer_sk AS ctr_customer_sk, " +
                "       sr_store_sk AS ctr_store_sk, " +
                "       SUM(sr_fee) AS ctr_total_return " +
                "FROM store_returns, date_dim " +
                "WHERE sr_returned_date_sk = d_date_sk " +
                "  AND d_year = 2000 " +
                "GROUP BY sr_customer_sk, sr_store_sk" +
                ") " +
                "SELECT c_customer_id " +
                "FROM customer_total_return ctr1, store, customer " +
                "WHERE ctr1.ctr_total_return > ( " +
                "    SELECT AVG(ctr_total_return) * 1.2 " +
                "    FROM customer_total_return ctr2 " +
                "    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk" +
                ") " +
                "AND s_store_sk = ctr1.ctr_store_sk " +
                "AND s_state = 'NM' " +
                "AND ctr1.ctr_customer_sk = c_customer_sk " +
                "ORDER BY c_customer_id " +
                "LIMIT 100";

        tableEnv.executeSql(query).print();
    }
}
