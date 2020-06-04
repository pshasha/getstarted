from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import year, month, concat, to_date, sum , dense_rank
from pyspark.sql.window import Window as w
from pyspark.sql.dataframe import DataFrameWriter as df


spark = SparkSession.builder.appName("top5products").getOrCreate()


def getdata(args0):
    data = spark.read.format("csv").load(args0)
    return data


# convert df with names:
orders_data = getdata("/home/shashank/PycharmProjects/getstarted/retail_data/orders/part-00000"). \
    toDF("order_id", "order_date", "order_customer_id", "order_status")

orderItems_data = getdata("/home/shashank/PycharmProjects/getstarted/retail_data/order_items/part-00000"). \
    toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity",
         "order_item_subtotal", "order_item_product_price")

products_data = getdata("/home/shashank/PycharmProjects/getstarted/retail_data/products/part-00000"). \
    toDF("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image")

# df with proper datatypes :

orders = orders_data.withColumn("order_id", orders_data.order_id.cast(IntegerType())) \
    .withColumn("order_customer_id", orders_data.order_customer_id.cast(IntegerType()))

orderItems = orderItems_data.withColumn("order_item_id", orderItems_data.order_item_id.cast(IntegerType())) \
    .withColumn("order_item_order_id", orderItems_data.order_item_order_id.cast(IntegerType())) \
    .withColumn("order_item_product_id", orderItems_data.order_item_product_id.cast(IntegerType())). \
    withColumn("order_item_quantity", orderItems_data.order_item_quantity.cast(IntegerType())) \
    .withColumn("order_item_subtotal", orderItems_data.order_item_subtotal.cast(FloatType())) \
    .withColumn("order_item_product_price", orderItems_data.order_item_product_price.cast(FloatType()))

products = products_data.withColumn("product_id", products_data.product_id.cast(IntegerType())). \
    withColumn("product_category_id", products_data.product_category_id.cast(IntegerType())) \
    .withColumn("product_price", products_data.product_price.cast(FloatType()))

data = orders.join(orderItems, orders.order_id == orderItems.order_item_id). \
    join(products, orderItems.order_item_product_id == products.product_id). \
    select("product_id", "product_name", "order_item_subtotal", "order_date"). \
    withColumn("Month", concat(year(to_date(orders.order_date)), month(to_date(orders.order_date))).cast(IntegerType()))

getRevenue = data.select(data.product_id, data.product_name, data.order_item_subtotal, data.Month).\
             groupBy(data.product_id, data.product_name,data.Month).agg(sum(data.order_item_subtotal).alias("Revenue"))

wf = w.partitionBy(getRevenue.Month).orderBy(getRevenue.Revenue.desc())

result = getRevenue.withColumn("Rank", dense_rank().over(wf))

Final_result = result.where(result.Rank <= 5)

print(type(Final_result))












