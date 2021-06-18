-- In order to use Pig with Phoenix, we need to tell Pig where to get the client library for Phoenix
REGISTER /usr/hdp/current/phoenix-client/phoenix-client.jar

-- Load u.user into users
users = LOAD '/user/maria_dev/ml-100k/u.user' 
USING PigStorage('|') 
AS (USERID:int, AGE:int, GENDER:chararray, OCCUPATION:chararray, ZIP:chararray);

-- Store users into HBase thru Phoenix
-- batch size: gather these many data before dumping into disk. Should make sure we have the memory to hold 5000 entries.
-- Prior to dumping data through Phoenix, the users table needs to be created through Phoenix beforehand.
STORE users into 'hbase://users' using
    org.apache.phoenix.pig.PhoenixHBaseStorage('localhost','-batchSize 5000');

-- Load the data back. Only loading he USERID and OCCUPATION fields.
occupations = load 'hbase://table/users/USERID,OCCUPATION' using org.apache.phoenix.pig.PhoenixHBaseLoader('localhost');

-- A grouping query.
grpd = GROUP occupations BY OCCUPATION; 
cnt = FOREACH grpd GENERATE group AS OCCUPATION,COUNT(occupations);
DUMP cnt;  

-- Since pig runs on top of MapReduce, there is a big overhead when running this script
-- $pig pig-over-phoenix.pig
