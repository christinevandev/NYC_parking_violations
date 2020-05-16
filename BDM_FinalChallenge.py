
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


# In[3]:


#NEW CHANGES
# cache dataframes
# change order in the join - put boro first
# don't use vio_rel and clean_rel
# filter years with an and statement


# In[4]:


# read data
# violations = spark.read.csv('pv_randomSample.csv', header=True, multiLine=True, escape='"', inferSchema=True)
# centerlines = spark.read.csv('Centerline.csv', header=True, multiLine=True, escape='"', inferSchema=True)
violations = spark.read.csv('pv_randomSample.csv', header=True, inferSchema=True)
centerlines = spark.read.csv('Centerline.csv', header=True, inferSchema=True)


# ### Preprocessing

# In[5]:


vio_rel = violations.select(violations['House Number'], upper(violations['Street Name']).alias('Street Name'), upper(violations['Violation County']).alias('Violation County'), violations['Issue Date'])
# extract year from date
vio_rel = vio_rel.withColumn('Year', vio_rel['Issue Date'].substr(-4,4))
# filter for relevant years
# vio_rel = vio_rel.filter((vio_rel["Year"]>"2014")&(vio_rel["Year"]<"2020"))
vio_rel = vio_rel.filter((vio_rel["Year"]=="2015")|(vio_rel["Year"] == "2016")|(vio_rel["Year"] == "2017")|(vio_rel["Year"] == "2018")|(vio_rel["Year"] == "2019"))


# In[6]:


cent_rel = centerlines.select(centerlines['PHYSICALID'], upper(centerlines['FULL_STREE']).alias('FULL_STREE'), upper(centerlines['ST_LABEL']).alias('ST_LABEL'), centerlines['BOROCODE'], centerlines['L_LOW_HN'], centerlines['L_HIGH_HN'], centerlines['R_LOW_HN'], centerlines['R_HIGH_HN'])


# In[7]:


# cache dataframes
vio_rel.cache()
cent_rel.cache()


# In[8]:


# drop rows with nulls
vio_rel = vio_rel.na.drop()
cent_rel = cent_rel.na.drop()


# In[9]:


# violation data preprocessing
#vio_clean = vio.withColumn("Clean House Number", regexp_replace(vio["House Number"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
vio_rel = vio_rel.withColumn("Clean House Number", regexp_replace(vio_rel["House Number"], "[- ]", "").cast(IntegerType()))
vio_rel = vio_rel.na.drop()


# In[10]:





# In[11]:


# centerline data preprocessing
# clean the house number ranges
# cent_clean = cent.withColumn("Clean L_LOW_HN", regexp_replace(cent["L_LOW_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
# cent_clean = cent_clean.withColumn("Clean L_HIGH_HN", regexp_replace(cent["L_HIGH_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
# cent_clean = cent_clean.withColumn("Clean R_LOW_HN", regexp_replace(cent["R_LOW_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
# cent_clean = cent_clean.withColumn("Clean R_HIGH_HN", regexp_replace(cent["R_HIGH_HN"], "[- ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]", "").cast(IntegerType()))
cent_rel = cent_rel.withColumn("Clean L_LOW_HN", regexp_replace(cent_rel["L_LOW_HN"], "[- ]", "").cast(IntegerType()))
cent_rel = cent_rel.withColumn("Clean L_HIGH_HN", regexp_replace(cent_rel["L_HIGH_HN"], "[- ]", "").cast(IntegerType()))
cent_rel = cent_rel.withColumn("Clean R_LOW_HN", regexp_replace(cent_rel["R_LOW_HN"], "[- ]", "").cast(IntegerType()))
cent_rel = cent_rel.withColumn("Clean R_HIGH_HN", regexp_replace(cent_rel["R_HIGH_HN"], "[- ]", "").cast(IntegerType()))
# drop any rows with nulls
cent_rel = cent_rel.na.drop()


# In[12]:





# In[13]:


# county to borocode mapping for violation dataset
# 1  = Manhattan: MAN,MH,MN,NEWY,NEW Y,NY
# 2  = Bronx: BRONX,BX
# 3  = Brooklyn: BK,K,KING,KINGS
# 4  = Queens: Q,QN,QNS,QU,QUEEN
# 5  = Staten Island: R,RICHMOND
vio_rel = vio_rel.withColumn("MappedBorocode", f.when((f.col('Violation County') == "MAN") | (f.col('Violation County') == "MH") | (f.col('Violation County') == "MN") | (f.col('Violation County') == "NEWY") | (f.col('Violation County') == "NEW Y") | (f.col('Violation County') == "NY"),"1").when((f.col('Violation County') == "BX") | (f.col('Violation County') == "BRONX"), "2").when((f.col('Violation County') == "BK") | (f.col('Violation County') == "K") | (f.col('Violation County') == "KING") | (f.col('Violation County') == "KINGS"), "3").when((f.col('Violation County') == "Q") | (f.col('Violation County') == "QN") | (f.col('Violation County') == "QNS") | (f.col('Violation County') == "QU") | (f.col('Violation County') == "QUEEN"),"4").when((f.col('Violation County') == "R") | (f.col('Violation County') == "RICHMOND"), "5"))
vio_rel = vio_rel.na.drop() #in case there were any incorrect counties recorded



# In[14]:


vio_rel.show()


# In[15]:


# add IsEven
vio_rel = vio_rel.withColumn("IsEven", f.when((f.col('Clean House Number')%2 == 0), "yes").otherwise("no"))
vio_rel.show()


# In[16]:


cent_rel.dtypes


# In[17]:


vio_rel.dtypes


# In[18]:


# drop unused columns before the join
#df = df.drop("address", "phoneNumber")
vio_rel = vio_rel.drop("House Number","Issue Date","Violation County")
cent_rel = cent_rel.drop("L_LOW_HN","L_HIGH_HN","R_LOW_HN","R_HIGH_HN")


# In[19]:


vio_rel.show()


# In[20]:


cent_rel.show()


# ### Union

# In[30]:


c1 = cent_rel.drop("FULL_STREE")
c2 = cent_rel.drop("ST_LABEL")


# In[ ]:


(violations['Street Name']).alias('Street Name')


# In[31]:


c1.show()


# In[48]:


vio_rel.show()


# In[32]:


c2.show()


# In[39]:


c3 = c1.union(c2).distinct().sort("PHYSICALID")
c3.show()


# In[40]:





# In[37]:





# In[38]:





# ## Join

# In[41]:


from pyspark.sql.functions import broadcast
#df = vio_clean.join(broadcast(cent_clean), (((vio_clean["Street Name"]==cent_clean["FULL_STREE"])|(vio_clean["Street Name"]==cent_clean["FULL_STREE"]))&(vio_clean["MappedBorocode"]==cent_clean["Borocode"])&((vio_clean["IsEven"]=="yes")&(vio_clean["Clean House Number"]<=cent_clean["Clean R_HIGH_HN"])&(vio_clean["Clean House Number"]>=cent_clean["Clean R_Low_HN"]))))


# In[42]:


df = vio_rel.join(broadcast(c3), 
                    (
                        (vio_rel["MappedBorocode"]==c3["Borocode"])
                        &(vio_rel["Street Name"]==c3["ST_LABEL"])
                        &(((vio_rel["IsEven"]=="yes")&(vio_rel["Clean House Number"]<=c3["Clean R_HIGH_HN"])&(vio_rel["Clean House Number"]>=c3["Clean R_LOW_HN"]))|((vio_rel["IsEven"]=="no")&(vio_rel["Clean House Number"]<=c3["Clean L_HIGH_HN"])&(vio_rel["Clean House Number"]>=c3["Clean L_LOW_HN"])))
                    )
                   )


# In[43]:


# cache dataframe
df.cache()


# In[44]:


df.dtypes


# In[45]:





# ## Post Processing

# In[151]:

vio_rel.unpersist()
clean_rel.unpersist()
output = df.select(df['PHYSICALID'], df['Year']).groupBy("PHYSICALID").pivot("Year",["2015","2016","2017","2018","2019"]).count().sort("PHYSICALID").na.fill(0)


# In[153]:




# In[154]:


output.show(5)


# In[399]:


# add physical IDs with no violation back


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# TO DO: groupBy house number/street and count before the join
# Fix the compound house issues
# add




