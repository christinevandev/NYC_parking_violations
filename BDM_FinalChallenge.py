
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import upper
from pyspark.sql.functions import size
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as f

sc = SparkContext()
spark = SparkSession(sc)

# read data
violations = spark.read.csv('hdfs:///tmp/bdm/nyc_parking_violation/', header=True, inferSchema=True)
centerlines = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv', header=True, inferSchema=True)

vio_rel = violations.select(violations['House Number'], upper(violations['Street Name']).alias('Street Name'), upper(violations['Violation County']).alias('Violation County'), violations['Issue Date'])
# extract year from date
vio_rel = vio_rel.withColumn('Year', vio_rel['Issue Date'].substr(-4,4))
# filter for relevant years
vio_rel = vio_rel.filter((vio_rel["Year"]=="2015")|(vio_rel["Year"] == "2016")|(vio_rel["Year"] == "2017")|(vio_rel["Year"] == "2018")|(vio_rel["Year"] == "2019"))

cent_rel = centerlines.select(centerlines['PHYSICALID'], upper(centerlines['FULL_STREE']).alias('FULL_STREE'), upper(centerlines['ST_LABEL']).alias('ST_LABEL'), centerlines['BOROCODE'], centerlines['L_LOW_HN'], centerlines['L_HIGH_HN'], centerlines['R_LOW_HN'], centerlines['R_HIGH_HN'])

# cache dataframes
vio_rel.cache()
cent_rel.cache()

# drop rows with nulls
vio_rel = vio_rel.na.drop()
cent_rel = cent_rel.na.drop()

# violation data preprocessing
vio_rel = vio_rel.withColumn("Clean House Number", regexp_replace(vio_rel["House Number"], "[- ]", "").cast(IntegerType()))
vio_rel = vio_rel.na.drop()

# centerline data preprocessing
cent_rel = cent_rel.withColumn("Clean L_LOW_HN", regexp_replace(cent_rel["L_LOW_HN"], "[- ]", "").cast(IntegerType()))
cent_rel = cent_rel.withColumn("Clean L_HIGH_HN", regexp_replace(cent_rel["L_HIGH_HN"], "[- ]", "").cast(IntegerType()))
cent_rel = cent_rel.withColumn("Clean R_LOW_HN", regexp_replace(cent_rel["R_LOW_HN"], "[- ]", "").cast(IntegerType()))
cent_rel = cent_rel.withColumn("Clean R_HIGH_HN", regexp_replace(cent_rel["R_HIGH_HN"], "[- ]", "").cast(IntegerType()))
# drop any rows with nulls
cent_rel = cent_rel.na.drop()

# county to borocode mapping for violation dataset
# 1  = Manhattan: MAN,MH,MN,NEWY,NEW Y,NY
# 2  = Bronx: BRONX,BX
# 3  = Brooklyn: BK,K,KING,KINGS
# 4  = Queens: Q,QN,QNS,QU,QUEEN
# 5  = Staten Island: R,RICHMOND
vio_rel = vio_rel.withColumn("MappedBorocode", f.when((f.col('Violation County') == "MAN") | (f.col('Violation County') == "MH") | (f.col('Violation County') == "MN") | (f.col('Violation County') == "NEWY") | (f.col('Violation County') == "NEW Y") | (f.col('Violation County') == "NY"),"1").when((f.col('Violation County') == "BX") | (f.col('Violation County') == "BRONX"), "2").when((f.col('Violation County') == "BK") | (f.col('Violation County') == "K") | (f.col('Violation County') == "KING") | (f.col('Violation County') == "KINGS"), "3").when((f.col('Violation County') == "Q") | (f.col('Violation County') == "QN") | (f.col('Violation County') == "QNS") | (f.col('Violation County') == "QU") | (f.col('Violation County') == "QUEEN"),"4").when((f.col('Violation County') == "R") | (f.col('Violation County') == "RICHMOND"), "5"))
vio_rel = vio_rel.na.drop() #in case there were any incorrect counties recorded

# add IsEven
vio_rel = vio_rel.withColumn("IsEven", f.when((f.col('Clean House Number')%2 == 0), "yes").otherwise("no"))

# drop unused columns before the join
vio_rel = vio_rel.drop("House Number","Issue Date","Violation County")
cent_rel = cent_rel.drop("L_LOW_HN","L_HIGH_HN","R_LOW_HN","R_HIGH_HN")

# use a union to optimize join
c1 = cent_rel.drop("FULL_STREE")
c2 = cent_rel.drop("ST_LABEL")
c3 = c1.union(c2).distinct().sort("PHYSICALID")

# join
from pyspark.sql.functions import broadcast
df = vio_rel.join(broadcast(c3), 
                    (
                        (vio_rel["MappedBorocode"]==c3["Borocode"])
                        &(vio_rel["Street Name"]==c3["ST_LABEL"])
                        &(((vio_rel["IsEven"]=="yes")&(vio_rel["Clean House Number"]<=c3["Clean R_HIGH_HN"])&(vio_rel["Clean House Number"]>=c3["Clean R_LOW_HN"]))|((vio_rel["IsEven"]=="no")&(vio_rel["Clean House Number"]<=c3["Clean L_HIGH_HN"])&(vio_rel["Clean House Number"]>=c3["Clean L_LOW_HN"])))
                    )
                   )

# cache dataframe
df.cache()

# post processing
output = df.select(df['PHYSICALID'], df['Year']).groupBy("PHYSICALID").pivot("Year",["2015","2016","2017","2018","2019"]).count().sort("PHYSICALID").na.fill(0)
output.cache()

# use OLS to calculate slope
import statsmodels.api as sm
output = output.withColumn('slope', sm.OLS([output['2015'],output['2016'],output['2017'],output['2018'],output['2019']],[1,2,3,4,5]).fit().params[0])

# add physical IDs with no violations back
new_cent = cent_rel.select("PHYSICALID").withColumnRenamed('PHYSICALID','allIDS')

cond = [new_cent.allIDS == output.PHYSICALID]
finaldf = new_cent.join(broadcast(output), cond,'left_outer').drop('PHYSICALID').na.fill(0).sort('allIDS')

finaldf.write.csv('cmv2')
finaldf.show()



