from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# To run this script in Ambari:
# cd into /var/lib/ambari-server/resources/stacks/HDP/2.6/services
# clone this: git clone https://github.com/nikunjness/mongo-ambari.git
# restart ambari: sudo service ambari-server restart
# Login from a browser as admin, and then add MongoDB service.
# install pymongo: pip install pymongo

# Run: spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 spark-on-top-of-mongodb.py

# because MongoDB creates _id automatically, running this script again will result in a copy of the data loaded into MongoDB again.

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    # Convert it to a RDD of Row objects with (userID, age, gender, occupation, zip)
    users = lines.map(parseInput)
    # Convert that to a DataFrame
    usersDataset = spark.createDataFrame(users)

    # Write it into MongoDB
    usersDataset.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri","mongodb://127.0.0.1/movielens.users")\
        .mode('append')\
        .save()

    # Read it back from MongoDB into a new Dataframe
    readUsers = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri","mongodb://127.0.0.1/movielens.users")\
    .load() # This doesn't actually load the data into memory, loading only happens on an action call for Spark.

    readUsers.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show() # This is an action. MongoDB connector actually translates the SQL query into MongoDB query, and direclty executes it on MongoDB.

    # Stop the session
    spark.stop()
