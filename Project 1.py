# Databricks notebook source
# DBTITLE 1,dataset location
/FileStore/tables/sales_csv.txt


/FileStore/tables/menu_csv.txt

# COMMAND ----------

# DBTITLE 1,sales dataframe 
#create dataframe 

from pyspark.sql.types import StructType , StructField, IntegerType , StringType , DateType 
schema = StructType([StructField("product_id" , IntegerType() , True),
                     StructField("customer_id" , StringType() , True),
                     StructField("order_date" , DateType() , True),
                     StructField("location" , StringType() , True),
                     StructField("source_order" , StringType() , True),
                     ])

#sales_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv.txt") 
sales_df=spark.read.format("csv").schema(schema).load("/FileStore/tables/sales_csv.txt") 

display(sales_df)

sales_df.printSchema()

# COMMAND ----------

# DBTITLE 1,deriving year , month, quarter
from pyspark.sql.functions import month , year , quarter

sales_df= sales_df.withColumn("order_year",year(sales_df.order_date))

display(sales_df)

# COMMAND ----------

# DBTITLE 1,deriving month , quarter 

sales_df= sales_df.withColumn("order_month",month(sales_df.order_date))

sales_df= sales_df.withColumn("order_quarter",quarter(sales_df.order_date))

display(sales_df)

# COMMAND ----------

# DBTITLE 1,menu dataframe
#create dataframe 

from pyspark.sql.types import StructType , StructField, IntegerType , StringType , DateType 
schema = StructType([StructField("product_id" , IntegerType() , True),
                     StructField("product_name" , StringType() , True),
                     StructField("price" , StringType() , True)
                     ])

#sales_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv.txt") 
menu_df=spark.read.format("csv").schema(schema).load("/FileStore/tables/menu_csv.txt") 

display(menu_df)

menu_df.printSchema()

# COMMAND ----------

# DBTITLE 1,convert price to integer column
from pyspark.sql.functions import col

# Assuming you have already defined the schema and loaded the DataFrame menu_df

# Convert "price" column to integer type
menu_df = menu_df.withColumn("price", col("price").cast("integer"))

# Show the updated DataFrame schema
menu_df.printSchema()

# Show the first few rows of the DataFrame
menu_df.show()


# COMMAND ----------

# DBTITLE 1,total amount spent by each customer 
total_amount_spent = sales_df.join(menu_df, sales_df.product_id==menu_df.product_id,'left').groupBy('customer_id').sum('price').orderBy('customer_id')

display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,total amount spent by each food category
total_amount_each_food = sales_df.join(menu_df, sales_df.product_id==menu_df.product_id,'left').groupBy('product_name').sum("price").orderBy("product_name")

display(total_amount_each_food)

# COMMAND ----------

# DBTITLE 1,total amount of sale in each month 
total_amount_each_month = sales_df.join(menu_df, sales_df.product_id==menu_df.product_id,'left').groupBy('order_month').sum("price").orderBy("order_month")

display(total_amount_each_month)

# COMMAND ----------

# DBTITLE 1,yearly sales 
yearly_sales = sales_df.join(menu_df, sales_df.product_id==menu_df.product_id,'left').groupBy('order_year').sum("price").orderBy("order_year")

display(yearly_sales)


# COMMAND ----------

# DBTITLE 1,yearly sales 
qua_sales = sales_df.join(menu_df, sales_df.product_id==menu_df.product_id,'left').groupBy('order_quarter').sum("price").orderBy("order_quarter")

display(qua_sales)

# COMMAND ----------

# DBTITLE 1,How many times each product purchased
from pyspark.sql.functions import count

each_product_purchase = sales_df.join(menu_df, sales_df.product_id == menu_df.product_id, 'left') \
    .groupBy(menu_df.product_id, "product_name") \
    .agg(count(menu_df.product_id).alias("purchased")) \
    .orderBy(col("purchased").desc()).drop("product_id")

display(each_product_purchase)


# COMMAND ----------

# DBTITLE 1,top 5 order item
from pyspark.sql.functions import count

top_5= sales_df.join(menu_df, sales_df.product_id == menu_df.product_id, 'left') \
    .groupBy(menu_df.product_id, "product_name") \
    .agg(count(menu_df.product_id).alias("purchased")) \
    .orderBy(col("purchased").desc()).drop("product_id").limit(5)

display(top_5)

# COMMAND ----------

# DBTITLE 1,top order 
from pyspark.sql.functions import count

top_1= sales_df.join(menu_df, sales_df.product_id == menu_df.product_id, 'left') \
    .groupBy(menu_df.product_id, "product_name") \
    .agg(count(menu_df.product_id).alias("purchased")) \
    .orderBy(col("purchased").desc()).drop("product_id").limit(1)

display(top_1)

# COMMAND ----------

# DBTITLE 1,frequency of customer visiting restaurent
from pyspark.sql.functions import countDistinct

frequency_of_customer =sales_df.filter(sales_df.source_order=='Restaurant').groupBy("customer_id").agg(countDistinct("order_date")).orderBy("customer_id")

display(frequency_of_customer )

# COMMAND ----------

# DBTITLE 1,Total Sales by country
from pyspark.sql.functions import count
sales_countrywise= sales_df.groupBy("location").agg(count("location").alias("total_sales")).orderBy("location")

display(sales_countrywise)

# COMMAND ----------

# DBTITLE 1,Total Sales by source order
from pyspark.sql.functions import count

sales_source= sales_df.groupBy("source_order").agg(count("source_order").alias("source_order_total_sales")).orderBy("source_order")

display(sales_source)

# COMMAND ----------


