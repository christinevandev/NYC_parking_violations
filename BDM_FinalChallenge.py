#!/usr/bin/env python
# coding: utf-8

# In[334]:


from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import format_number
from pyspark.sql.functions import lower
from pyspark.sql.functions import upper
from pyspark.sql.functions import year
from pyspark.sql.functions import split
from pyspark.sql.functions import slice
from pyspark.sql.functions import size
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as f
spark = SparkSession.builder.getOrCreate()

# read data
# violations = spark.read.csv('pv_randomSample.csv', header=True, multiLine=True, escape='"', inferSchema=True)
# centerlines = spark.read.csv('Centerline.csv', header=True, multiLine=True, escape='"', inferSchema=True)
violations = spark.read.csv('hdfs:///tmp/bdm/nyc_parking_violation/', header=True, inferSchema=True)
centerlines = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv', header=True, inferSchema=True)


vio_rel = violations.select(violations['House Number'], upper(violations['Street Name']).alias('Street Name'), upper(violations['Violation County']).alias('Violation County'), violations['Issue Date'])
# extract year from date
vio_rel = vio_rel.withColumn('Year', vio_rel['Issue Date'].substr(-4,4))
# filter for relevant years
vio_rel = vio_rel.filter((vio_rel["Year"]=="2015")|(vio_rel["Year"] == "2016")|(vio_rel["Year"] == "2017")|(vio_rel["Year"] == "2018")|(vio_rel["Year"] == "2019"))

cent_rel = centerlines.select(centerlines['PHYSICALID'], upper(centerlines['FULL_STREE']).alias('FULL_STREE'), upper(centerlines['ST_LABEL']).alias('ST_LABEL'), centerlines['BOROCODE'], centerlines['L_LOW_HN'], centerlines['L_HIGH_HN'], centerlines['R_LOW_HN'], centerlines['R_HIGH_HN'])


# drop rows with nulls
vio = vio_rel.na.drop()
cent = cent_rel.na.drop()

# violation data preprocessing
#vio_clean = vio.withColumn("Clean House Number", regexp_replace(vio["House Number"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
vio_clean = vio.withColumn("Clean House Number", regexp_replace(vio["House Number"], "[- ]", "").cast(IntegerType()))
vio_clean = vio_clean.na.drop()

# centerline data preprocessing
# clean the house number ranges
# cent_clean = cent.withColumn("Clean L_LOW_HN", regexp_replace(cent["L_LOW_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
# cent_clean = cent_clean.withColumn("Clean L_HIGH_HN", regexp_replace(cent["L_HIGH_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
# cent_clean = cent_clean.withColumn("Clean R_LOW_HN", regexp_replace(cent["R_LOW_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
# cent_clean = cent_clean.withColumn("Clean R_HIGH_HN", regexp_replace(cent["R_HIGH_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
cent_clean = cent.withColumn("Clean L_LOW_HN", regexp_replace(cent["L_LOW_HN"], "[- ]", "").cast(IntegerType()))
cent_clean = cent_clean.withColumn("Clean L_HIGH_HN", regexp_replace(cent["L_HIGH_HN"], "[- ]", "").cast(IntegerType()))
cent_clean = cent_clean.withColumn("Clean R_LOW_HN", regexp_replace(cent["R_LOW_HN"], "[- ]", "").cast(IntegerType()))
cent_clean = cent_clean.withColumn("Clean R_HIGH_HN", regexp_replace(cent["R_HIGH_HN"], "[- ]", "").cast(IntegerType()))
# drop any rows with nulls
cent_clean = cent_clean.na.drop()


# county to borocode mapping for violation dataset
# 1  = Manhattan: MAN,MH,MN,NEWY,NEW Y,NY
# 2  = Bronx: BRONX,BX
# 3  = Brooklyn: BK,K,KING,KINGS
# 4  = Queens: Q,QN,QNS,QU,QUEEN
# 5  = Staten Island: R,RICHMOND
vio_clean = vio_clean.withColumn("MappedBorocode", f.when((f.col('Violation County') == "MAN") | (f.col('Violation County') == "MH") | (f.col('Violation County') == "MN") | (f.col('Violation County') == "NEWY") | (f.col('Violation County') == "NEW Y") | (f.col('Violation County') == "NY"),"1").when((f.col('Violation County') == "BX") | (f.col('Violation County') == "BRONX"), "2").when((f.col('Violation County') == "BK") | (f.col('Violation County') == "K") | (f.col('Violation County') == "KING") | (f.col('Violation County') == "KINGS"), "3").when((f.col('Violation County') == "Q") | (f.col('Violation County') == "QN") | (f.col('Violation County') == "QNS") | (f.col('Violation County') == "QU") | (f.col('Violation County') == "QUEEN"),"4").when((f.col('Violation County') == "R") | (f.col('Violation County') == "RICHMOND"), "5"))
vio_clean = vio_clean.na.drop() #in case there were any incorrect counties recorded


# add IsEven
vio_clean = vio_clean.withColumn("IsEven", f.when((f.col('Clean House Number')%2 == 0), "yes").otherwise("no"))

# drop unused columns before the join
vio_clean = vio_clean.drop("House Number","Issue Date","Violation County")
cent_clean = cent_clean.drop("L_LOW_HN","L_HIGH_HN","R_LOW_HN","R_HIGH_HN")

from pyspark.sql.functions import broadcast

df = vio_clean.join(broadcast(cent_clean), 
                    (
                        ((vio_clean["Street Name"]==cent_clean["FULL_STREE"])|(vio_clean["Street Name"]==cent_clean["ST_LABEL"]))
                        &(vio_clean["MappedBorocode"]==cent_clean["Borocode"])
                        &(((vio_clean["IsEven"]=="yes")&(vio_clean["Clean House Number"]<=cent_clean["Clean R_HIGH_HN"])&(vio_clean["Clean House Number"]>=cent_clean["Clean R_LOW_HN"]))|((vio_clean["IsEven"]=="no")&(vio_clean["Clean House Number"]<=cent_clean["Clean L_HIGH_HN"])&(vio_clean["Clean House Number"]>=cent_clean["Clean L_LOW_HN"])))
                    )
                   )


df.select(df['PHYSICALID'], df['Year']).groupBy("PHYSICALID").pivot("Year",["2015","2016","2017","2018","2019"]).count().sort("PHYSICALID").na.fill(0).write.csv('test')


















