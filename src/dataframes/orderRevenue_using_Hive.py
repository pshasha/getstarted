from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

# Initialise spark
spark = SparkSession. \
    Builder(). \
    appName("Revenue using Hive"). \
    enableHiveSupport(). \
    getOrCreate()


# load the data from local to dataframes

def getdata(args0):
    data = spark.read.format("csv").load(args0)
    return data


# convert df with names:
orders_data = getdata("/home/shashank/PycharmProjects/getstarted/retail_data/orders/part-00000"). \
    toDF("order_id", "order_date", "order_customer_id", "order_status")

orderItems_data = getdata("/home/shashank/PycharmProjects/getstarted/retail_data/order_items/part-00000"). \
    toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity",
         "order_item_subtotal", "order_item_product_price")

orders = orders_data.withColumn("order_id", orders_data.order_id.cast(IntegerType())) \
    .withColumn("order_customer_id", orders_data.order_customer_id.cast(IntegerType()))

orderItems = orderItems_data.withColumn("order_item_id", orderItems_data.order_item_id.cast(IntegerType())) \
    .withColumn("order_item_order_id", orderItems_data.order_item_order_id.cast(IntegerType())) \
    .withColumn("order_item_product_id", orderItems_data.order_item_product_id.cast(IntegerType())). \
    withColumn("order_item_quantity", orderItems_data.order_item_quantity.cast(IntegerType())) \
    .withColumn("order_item_subtotal", orderItems_data.order_item_subtotal.cast(FloatType())) \
    .withColumn("order_item_product_price", orderItems_data.order_item_product_price.cast(FloatType()))

# creating databases :
spark.sql("DROP DATABASE IF EXISTS retail_db")
spark.sql("CREATE DATABASE retail_db")
spark.sql("use retail_db")

# creating tables from dataframes :
orders.registerTempTable("orders")
orderItems.registerTempTable("orderItems")

result = spark.sql("SELECT O.order_id, SUM(OI.order_item_subtotal) AS revenue "
                   "FROM orders O JOIN orderItems OI "
                   "ON O.order_id = OI.order_item_order_id "
                   "WHERE O.order_status IN ('COMPLETE','CLOSED') "
                   "GROUP BY O.order_id")




