# Apache Cassandra & Spark SQL Example

This GitHub repository showcases a Spark SQL project that focuses on data handling with Cassandra database integration. 
<img src="https://github.com/Mansouri-Anas/CassandraSparkApplication/assets/106403012/c0f42c0f-1c2e-42c7-9812-e9421cd773c6" alt="Cassandra Logo" width="500">

## Getting Started

1. To run this project and avoid any dependency mismatch, make sure to have the following dependencies in your project

```shell

<dependencies>
        <dependency>
            <groupId>com.datastax.spark</groupId>
            <artifactId>spark-cassandra-connector_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>com.github.jnr</groupId>
            <artifactId>jnr-posix</artifactId>
            <version>3.1.16</version>
        </dependency>
        <dependency>
            <groupId>joda-time</groupId>
            <artifactId>joda-time</artifactId>
            <version>2.12.5</version>
        </dependency>
    </dependencies>
```
2. Start your cassandra server in Ubuntu :

```shell
cqlsh
```

3. Create the example "Bank" database :

```shell
// Create Keyspace  : 
 create keyspace bank with replication = {'class':'SimpleStrategy','replication_factor':1};
 use bank;
// Create tables

CREATE TABLE bank.customers (     
 id text PRIMARY KEY,    
 county text,     
 name text );

CREATE TABLE bank.transactions (
    customerid text,
    year int,
    month int,
    id timeuuid,
    amount int,
    card text,
    status text,
    PRIMARY KEY ((customerid, year, month), id)
);

CREATE TABLE bank.balance (
    customerid text,
    card text,
    balance int,
    updated_at timestamp,
    PRIMARY KEY ((customerid, card))
);


INSERT INTO bank.customers (id, county, name) VALUES ('1', 'County A', 'John Doe');
INSERT INTO bank.customers (id, county, name) VALUES ('2', 'County B', 'Jane Smith');
INSERT INTO bank.customers (id, county, name) VALUES ('3', 'County C', 'David Johnson');

INSERT INTO bank.transactions (customerid, year, month, id, amount, card, status) VALUES ('1', 2023, 6, now(), 100, 'Card123', 'Approved');
INSERT INTO bank.transactions (customerid, year, month, id, amount, card, status) VALUES ('2', 2023, 6, now(), 200, 'Card456', 'Declined');
INSERT INTO bank.transactions (customerid, year, month, id, amount, card, status) VALUES ('3', 2023, 6, now(), 150, 'Card789', 'Approved');

INSERT INTO bank.balance (customerid, card, balance, updated_at) VALUES ('1', 'Card123', 500, toTimestamp(now()));
INSERT INTO bank.balance (customerid, card, balance, updated_at) VALUES ('2', 'Card456', 1000, toTimestamp(now()));
INSERT INTO bank.balance (customerid, card, balance, updated_at) VALUES ('3', 'Card789', 750, toTimestamp(now()));

```

4. Run the CassandraSpark Class : 

```shell
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


        // Select number of transactions per by year and month

        Dataset<Row> result1 = dfTransactions
                .groupBy("year", "month")
                .count()
                .withColumnRenamed("count", "num_transactions");
                
        // Select Customers names that have a balance higher thnn 500

        Dataset<Row> result2 = dfBalance
                .join(dfCustomers, dfBalance.col("customerid").equalTo(dfCustomers.col("id")))
                .filter(dfBalance.col("balance").gt(500))
                .select(dfCustomers.col("name").alias("customer_name"), dfBalance.col("balance"));

        // Show the results
        result1.show();
        result2.show();

        // Stop the SparkSession
        spark.stop();


    }
}
```
## Contributing
Contributions to this project are always welcome. If you find any issues or have ideas for improvements, please open an issue or submit a pull request.

## License
This project is licensed under the MIT License. Feel free to use and modify the code according to your needs.
