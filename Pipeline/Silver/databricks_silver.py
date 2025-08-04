# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC Data Access using App
# MAGIC

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.adstorage1234.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adstorage1234.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adstorage1234.dfs.core.windows.net", "XXXXXXXXXXXXXXXXXXXXXXXXX")
spark.conf.set("fs.azure.account.oauth2.client.secret.adstorage1234.dfs.core.windows.net", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adstorage1234.dfs.core.windows.net", "XXXXXXXXXXXXXXXXXXXXXXXXXX/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loding

# COMMAND ----------

# MAGIC %md 
# MAGIC Read Calender data 

# COMMAND ----------

df_cal=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cus=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------



# COMMAND ----------

df_procat=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_pro=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_ret=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sales=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_ter=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

df_subcat=spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@adstorage1234.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC displaying and manupulations on data 

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal=df.withColumn('Month', month(col('Date')))\
    .withColumn('Year',year(col('Date')))
df_cal.display()

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@adstorage1234.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

df_cus=df_cus.withColumn('FullName',concat(col('Prefix'),lit(" "),col('FirstName'),lit(" "),col('LastName')))

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@adstorage1234.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@adstorage1234.dfs.core.windows.net/AdventureWorks_Subcategories")\
    .save()

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro=df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
    .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@adstorage1234.dfs.core.windows.net/AdventureWorks_Products")\
    .save()

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@adstorage1234.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@adstorage1234.dfs.core.windows.net/AdventureWorks_Territories")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales=df_sales.withColumn('StockDate',to_timestamp(col('StockDate')))

# COMMAND ----------

df_sales=df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales=df_sales.withColumn('Multiply',col('OrderLineItem')*(col('OrderQuantity')))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Analysis

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@adstorage1234.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

df_sales.groupBy('OrderDate').agg((count('OrderNumber').alias('total_order'))).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

