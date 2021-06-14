from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# To run this script:
# $spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.1 spark-on-top-of-cassandra.py
# Refer to spark-cassandra-connector official documenation for the connector version:
# https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md

def parseInput(line):
    fields = line.split('|')
    # Important: Row field names MUST match up with the column names in Cassandra. 
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    # Convert it to a RDD of Row objects with (userID, age, gender, occupation, zip)
    users = lines.map(parseInput)
    # Convert that to a DataFrame
    usersDataset = spark.createDataFrame(users)

    # Write it into Cassandra
    usersDataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save() # this call to save() actually writes the data out to Cassandra.

    # Read it back from Cassandra into a new Dataframe
    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="users", keyspace="movielens")\
    .load() # load() actually connects the data frame to a Cassandra table. It doesn't act on loading the data untill an action function is called.

    # Create a view for running SQL queries.
    readUsers.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show()

    # Stop the session
    spark.stop()
