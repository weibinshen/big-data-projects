-- When creating this table on HBase, the table will be partitioned by its PRIMARY KEY
-- Phoenix is meant for analyzing HBase data using SQL. It can also support some OLTP queries.

CREATE TABLE IF NOT EXISTS us_population(
state CHAR(2) NOT NULL,
city VARCHAR NOT NULL,
population BIGINT
CONSTRAINT my_pk PRIMARY KEY (state, city));

-- To see what tables are available
!table 

-- UPSERT: Insert if not exist, otherwise update. There is no INSERT in Phoenix.
UPSERT INTO US_POPULATION VALUES ('NY', 'New York', 8143197);
UPSERT INTO US_POPULATION VALUES ('CA', 'Los Angeles', 3844829);
SELECT * FROM US_POPULATION;
SELECT * FROM US_POPULATION WHERE STATE='CA';

-- You can also do joins with Phoenix.

DROP TABLE US_POPULATION;

-- To quit:
!quit