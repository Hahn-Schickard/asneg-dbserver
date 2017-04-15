
#
# create test database
#
DROP DATABASE TestData;
CREATE DATABASE TestData;
SHOW DATABASES;

#
# create test table
#
USE TestData;
CREATE TABLE TestTable (
    Id varchar(255),
    Time DATETIME,
    Value1 double,
    Value2 double,
    Value3 double
); 
SHOW TABLES;

#
# insert data into table
#
INSERT INTO TestTable (Id, Time, Value1, Value2, Value3)
       VALUES ('Id1', '2017-04-04', '1.1', '1.2', '1.3'),
              ('Id2', '2017-04-04', '2.1', '2.2', '2.3'),
              ('Id3', '2017-04-04', '3.1', '3.2', '3.3'),
              ('Id4', '2017-04-04', '4.1', '4.2', '4.3'),
              ('Id5', '2017-04-04', '5.1', '5.2', '5.3'),
              ('Id6', '2017-04-04', '6.1', '6.2', '6.3'); 
SELECT * FROM TestTable;
