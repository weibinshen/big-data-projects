-- To import data from Pig to HBase, the table and column family needs to be created beforehand
-- [root@sandbox-hdp maria_dev]# hbase shell
-- In the command below, the first parameter is he table name, and the subsequent parameters are column family names.
-- hbase(main):002:0> create 'users','userinfo'
-- hbase(main):003:0> list
-- To run this script, use `pig hbase.pig`, then pig will kick-off mapReduce jobs under the hood to load the data into HBase.
-- Then back in HBase Shell, we can inspect the data using `hbase(main):003:0> scan 'users'`

-- Each cell in HBase is versioned, so it can retain different copies of values for one cell. So, a time-stamp is always appended to each version of data for a cell.

-- To delete a table, it must be disabled first
-- hbase(main):003:0> disable 'users'
-- hbase(main):004:0> drop 'users'

-- In the loaded table, the first column has to be the primary key in order to work properly with HBase.
users = LOAD '/user/maria_dev/ml-100k/u.user' 
USING PigStorage('|') 
AS (userID:int, age:int, gender:chararray, occupation:chararray, zip:int);

-- Here the primary key is implicit.
-- columns are represented in this syntax in HBase: <columnFamilyName>:<columnName>
STORE users INTO 'hbase://users' 
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage (
'userinfo:age,userinfo:gender,userinfo:occupation,userinfo:zip');
