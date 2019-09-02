from pyspark.sql import SparkSession

if __name__ == '__main__':
    hsc=SparkSession.builder.appName("marks in Hive").config("spark.sql.warehouse.dir","/user/hive/warehouse")\
        .enableHiveSupport().getOrCreate()
    hsc.sql("DROP TABLE IF EXISTS marks")
    hsc.sql("CREATE TABLE IF NOT EXISTS marks(name STRING, subject STRING, marks INT) row format delimited fields terminated by ',' ")
    hsc.sql("LOAD DATA LOCAL INPATH '/usr/local/spark/marks' INTO TABLE marks")
    marks_data=hsc.sql("select * from marks")
    marks_data.show()
    hsc.sql("select name,avg(marks) as AVG_MARKS from marks group by name").show()
