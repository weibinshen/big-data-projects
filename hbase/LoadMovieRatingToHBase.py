from starbase import Connection

# first start the rest service on the HBase cluster by: 
# /usr/hdp/current/hbase-master/bin/hbase-daemon.sh start rest -p 8000 --infoport 8001
# Also make sure to setup port forwarding of 8000 on HDP.
c = Connection("127.0.0.1", "8000")

ratings = c.table('ratings')

if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

# create function creates a "column family" that can contain a dynamic numbers of columns.
ratings.create('rating')

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("~/Desktop/ml-100k/u.data", "r")

batch = ratings.batch()

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingFile.close()

print ("Committing ratings data to HBase via REST service\n")
batch.commit(finalize=True)

print ("Get back ratings for some users...\n")
print ("Ratings for user ID 1:\n")
print (ratings.fetch("1"))
print ("Ratings for user ID 33:\n")
print (ratings.fetch("33"))

ratings.drop()
