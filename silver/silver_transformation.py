# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql.functions import *
from pyspark.sql.types import * 
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window 
from delta.tables import DeltaTable

# COMMAND ----------

import os
import sys

# COMMAND ----------

current_dir = os.getcwd()
sys.path.append(current_dir)

# COMMAND ----------

import importlib
import utils.custom_utils as custom_utils_module

importlib.reload(custom_utils_module)

from utils.custom_utils import transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##  # **Customers**

# COMMAND ----------

df_cust = spark.read.table("pysparkdbt.bronze.customers")

# COMMAND ----------

display(df_cust)

# COMMAND ----------

#Cleanup 1
df_cust = df_cust.withColumn("domain",split(col('email'),'@')[1])
display(df_cust)

# COMMAND ----------

#Cleanup 2
df_cust = df_cust.withColumn("phone_number",regexp_replace("phone_number",r"[^0-9]",""))
display(df_cust)

# COMMAND ----------

#Cleanup 3
df_cust = df_cust.withColumn("full_name",concat_ws(" ",col("first_name"),col("last_name")))
df_cust = df_cust.drop('first_name','last_name')
display(df_cust)

# COMMAND ----------

#Transformation 1
from utils.custom_utils import transformations

# COMMAND ----------

cust_obj = transformations()
cust_df_trns = cust_obj.dedup(df_cust,['customer_id'],'last_updated_timestamp')
display(cust_df_trns)

# COMMAND ----------

df_cust = cust_obj.process_timestamp(cust_df_trns)
display(df_cust)

# COMMAND ----------


if not spark.catalog.tableExists("pysparkdbt.silver.customers"):
    df_cust.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.customers")
else: 
    cust_obj.upsert(spark,df_cust,['customer_id'],'customers','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Count(*) From pysparkdbt.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drivers

# COMMAND ----------

df_driver = spark.read.table("pysparkdbt.bronze.drivers")
display(df_driver)

# COMMAND ----------

#Cleanup 1
df_driver = df_driver.withColumn("phone_number",regexp_replace("phone_number",r"[^0-9]",""))

#display(df_driver)

# COMMAND ----------

#Cleanup 2
df_driver = df_driver.withColumn("full_name",concat_ws(" ",col('first_name'),col('last_name')))
df_driver = df_driver.drop('first_name','last_name')

#display(df_driver)

# COMMAND ----------

driver_obj = transformations()

# COMMAND ----------

df_driver = driver_obj.dedup(df_driver,['driver_id'],'last_updated_timestamp')

# COMMAND ----------

df_driver = driver_obj.process_timestamp(df_driver)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.drivers"):
    df_driver.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.drivers")
else: 
    driver_obj.upsert(spark,df_driver,['driver_id'],'drivers','last_updated_timestamp')    

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from pysparkdbt.silver.drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Locations

# COMMAND ----------

df_loc = spark.read.table("pysparkdbt.bronze.locations")
display(df_loc)

# COMMAND ----------

loc_obj = transformations()

# COMMAND ----------

df_loc = loc_obj.dedup(df_loc,['location_id'],'last_updated_timestamp')
df_loc = loc_obj.process_timestamp(df_loc)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.locations"):
    df_loc.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.locations")
else:
    loc_obj.upsert(spark,df_loc,['location_id'],'locations','last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.locations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payments

# COMMAND ----------

df_pay = spark.read.table("pysparkdbt.bronze.payments")
display(df_pay)

# COMMAND ----------

#Transformations 1
df_pay = df_pay.withColumn("online_payment_status",
            when( ((col('payment_method')=='Card') & (col('payment_status')=='Success')), "online-success")
            .when( ((col('payment_method')=='Card') & (col('payment_status')=='Failed')), "online-failed")
            .when( ((col('payment_method')=='Card') & (col('payment_status')=='Pending')), "online-pending")
            .otherwise("offline")
            )
display(df_pay)                        

# COMMAND ----------

payment_obj = transformations()
df_pay = payment_obj.dedup(df_pay,['payment_id'],'last_updated_timestamp')
df_pay =  payment_obj.process_timestamp(df_pay)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.payments"):
    df_pay.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.payments")
else:
    payment_obj.upsert(spark,df_pay, ['payment_id'], 'payments', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vehicles

# COMMAND ----------

df_veh = spark.read.table("pysparkdbt.bronze.vehicles")
display(df_veh)

# COMMAND ----------

 #Transformations 1 
df_veh = df_veh.withColumn("make",upper(col("make")))
display(df_veh)

# COMMAND ----------

vehicle_obj = transformations()
df_veh = vehicle_obj.dedup(df_veh,['vehicle_id'],'last_updated_timestamp')
df_veh = vehicle_obj.process_timestamp(df_veh)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):
    df_veh.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.vehicles")
else:
    vehicle_obj.upsert(spark,df_veh, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.vehicles

# COMMAND ----------

