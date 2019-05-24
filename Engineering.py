# # Load the data
# 
# 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *


conf = SparkConf().setAppName("ds-for-telco")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
schema = StructType([     StructField("state", StringType(), True),     StructField("account_length", DoubleType(), True),     StructField("area_code", StringType(), True),     StructField("phone_number", StringType(), True),     StructField("intl_plan", StringType(), True),     StructField("voice_mail_plan", StringType(), True),     StructField("number_vmail_messages", DoubleType(), True),     StructField("total_day_minutes", DoubleType(), True),     StructField("total_day_calls", DoubleType(), True),     StructField("total_day_charge", DoubleType(), True),     StructField("total_eve_minutes", DoubleType(), True),     StructField("total_eve_calls", DoubleType(), True),     StructField("total_eve_charge", DoubleType(), True),     StructField("total_night_minutes", DoubleType(), True),     StructField("total_night_calls", DoubleType(), True),     StructField("total_night_charge", DoubleType(), True),     StructField("total_intl_minutes", DoubleType(), True),     StructField("total_intl_calls", DoubleType(), True),     StructField("total_intl_charge", DoubleType(), True),     StructField("number_customer_service_calls", DoubleType(), True),     StructField("churned", StringType(), True)])

churn_data = sqlContext.read     .format('com.databricks.spark.csv')     .load('/tmp/churn.all', schema = schema)


# # Basic DataFrame operations
# 
# Dataframes essentially allow you to express sql-like statements. We can filter, count, and so on. [DataFrame Operations documentation.](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations)

count = churn_data.count()
voice_mail_plans = churn_data.filter(churn_data.voice_mail_plan == " yes").count()
churned_customers = churn_data.filter(churn_data.churned == " True.").count()

"total: %d, voice mail plans: %d, churned customers: %d " % (count, voice_mail_plans, churned_customers)


# How many customers have  more than one service call? 

service_calls = churn_data.filter(churn_data.number_customer_service_calls > 1).count()
service_calls2 = churn_data.filter(churn_data.number_customer_service_calls > 2).count()

"customers with more than 1 service call: %d, 2 service calls: %d" % (service_calls, service_calls2)
sc.stop()