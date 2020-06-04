from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import *
from pyspark.sql.window import Window as w
#import configparser as cp
#import sys


#props = cp.RawConfigParser('/home/shashank/pycharm/pyspark/resources/application.properties')
#env = sys.argv()
#master(props.get(env,'executionMode'))

spark = SparkSession.builder.master("local").appName("top5productunderrevenue").getOrCreate()




orders_schema = StructType([StructField("order_id", IntegerType(), True, None),
                            StructField("order_date", StringType(), True, None),
                            StructField("order_customer_id", IntegerType(), True, None),
                            StructField("order_status", StringType(), True, None)])

order_items_Schema = StructType([StructField("order_item_id", IntegerType(), True, None),
                                 StructField("order_item_order_id", IntegerType(), True, None),
                                 StructField("order_item_product_id", IntegerType(), True, None),
                                 StructField("order_item_quantity", IntegerType(), True, None),
                                 StructField("order_item_subtotal", FloatType(), True, None),
                                 StructField("order_item_product_price", FloatType(), True, None)])

product_schema = StructType([StructField("product_id", IntegerType(), True),
                             StructField("product_category_id", IntegerType(), True),
                             StructField("product_name", StringType(), True),
                             StructField("product_description", StringType(), True),
                             StructField("product_price", FloatType(), True),
                             StructField("product_image", StringType(), True)])


def getfiles(args1, args2):
    data = spark.read.schema(args1).format("csv").load(args2)
    return data


orders = getfiles(orders_schema, "/home/shashank/pycharm/pyspark/retail_db/orders/part-00000")
orderItems = getfiles(order_items_Schema, "/home/shashank/pycharm/pyspark/retail_db/order_items/part-00000")
products = getfiles(product_schema, "/home/shashank/pycharm/pyspark/retail_db/products/part-00000")

ord_oi_prod = orders.join(orderItems, orders.order_id == orderItems.order_item_id).join(products,
                                                                                        orderItems.order_item_product_id == products.product_id). \
    select("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image",
           "order_item_subtotal",
           concat(year(orders.order_date), month(orders.order_date)).alias("Month").cast(IntegerType()))

getrevenue = ord_oi_prod.groupBy("product_id", "product_category_id", "product_name", "product_description",
                                 "product_price", "product_image", "Month") \
    .agg(round(sum("order_item_subtotal"), 1).alias("Revenue"))


from pyspark.sql.functions import rank,dense_rank

spec = w.partitionBy(getrevenue.Month).orderBy(getrevenue.Revenue.desc())

result = getrevenue.withColumn("Rank", dense_rank().over(spec))

result.show(2) 

