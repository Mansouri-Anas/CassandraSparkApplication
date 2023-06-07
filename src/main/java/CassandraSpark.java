import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class CassandraSpark {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder()
                .appName("Spark Cassandra Example")
                .master("local[*]") // Set the Spark master URL
                .config("spark.cassandra.connection.host", "localhost") // Replace with your Cassandra host
                .getOrCreate();

        Dataset<Row> dfCustomers = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "bank")
                .option("table", "customers")
                .load();

        Dataset<Row> dfBalance = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "bank")
                .option("table", "balance")
                .load();

        Dataset<Row> dfTransactions = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "bank")
                .option("table", "transactions")
                .load();


        // On prend le nombre de transactions par mois et année

        System.out.println("Nombre de transactions par mois et année");
        Dataset<Row> result1 = dfTransactions
                .groupBy("year", "month")
                .count()
                .withColumnRenamed("count", "num_transactions");

        System.out.println("Clients avec un solde > 500 ");

        Dataset<Row> result2 = dfBalance
                .join(dfCustomers, dfBalance.col("customerid").equalTo(dfCustomers.col("id")))
                .filter(dfBalance.col("balance").gt(500))
                .select(dfCustomers.col("name").alias("customer_name"), dfBalance.col("balance"));

        // Show the result
        result1.show();
        result2.show();

        // Stop the SparkSession
        spark.stop();


    }
}
