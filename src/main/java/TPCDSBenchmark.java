import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TPCDSBenchmark {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        try {
            // Read SQL from file
            String sqlCreateTables = new String(Files.readAllBytes(Paths.get("alltables.sql")));
            // Execute SQL
            tableEnv.executeSql(sqlCreateTables);
        } catch (IOException e) {
            e.printStackTrace();
        }

        executeQuery(tableEnv);
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
                "SELECT c.c_customer_id " +
                "FROM customer_total_return ctr1 " +
                "JOIN store s ON s.s_store_sk = ctr1.ctr_store_sk " +
                "JOIN customer c ON c.c_customer_sk = ctr1.ctr_customer_sk " +
                "WHERE ctr1.ctr_total_return > ( " +
                "    SELECT AVG(ctr_total_return) * 1.2 " +
                "    FROM customer_total_return ctr2 " +
                "    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk" +
                ") " +
                "AND s.s_state = 'NM' " +
                "ORDER BY c.c_customer_id " +
                "LIMIT 100";

        tableEnv.executeSql(query).print();

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
