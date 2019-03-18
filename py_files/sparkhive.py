# spark hive

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql import *

spark = SparkSession.builder.appName('Diamond_Mukesh').getOrCreate()

rdd1 = sc.textFile("wasb:///data/diamonds.csv")

# Find and remove the header from RDD
Header = rdd1.first()
rdd1 = rdd1.filter(lambda row:row != Header)
for i in rdd1.take(5):
    print(i)

schema = StructType([StructField('carat',StringType(),True),
                    StructField('cut',StringType(),True),
                    StructField('color',StringType(),True),
                    StructField('clarity',StringType(),True),
                    StructField('depth',StringType(),True),
                    StructField('table',StringType(),True),
                    StructField('price',StringType(),True),
                    StructField('col_x',StringType(),True),
                    StructField('col_y',StringType(),True),
                    StructField('col_z',StringType(),True)
                    ])
df1 = spark.createDataFrame(rdd1.map(lambda x:x.split(',')),schema)

print(df1.columns)
print(df1.printSchema())
print(df1.count())
print(len(df1.columns))

df1.registerTempTable('diamond_table')

# HIVE 
# diamond_data is raw table before partitioned table 
spark.sql("drop table if exists diamond_data")
spark.sql("create table diamond_data (carat string,cut string,color string,clarity string,depth string,table string, \
                           price string,colx string, \
                           coly string,colz string)  \
          stored as parquet")

spark.sql("insert into diamond_data select * from diamond_table")

# setting properties for dynamic partition 
spark.sql("set hive.exec.dynamic.partition=true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.max.dynamic.partition=10")
spark.sql("set hive.exec.max.dynamic.partition.pernode=10")
spark.sql("set hive.enforce.bucketing = true")

spark.sql("drop table if exists diamond_report")  # deleting the previous table 

# creating a new partitoned table 
spark.sql("create table diamond_report (carat string,color string,clarity string,depth string,table string, \
                           price string,colx string, \
                           coly string,colz string)  \
                           partitioned by (cut string)  \
                           stored as parquet")

# inserting into partitioned table from raw table
spark.sql("insert into diamond_report select carat,color,clarity,depth,table,price,colx,coly,colz,cut from diamond_data")