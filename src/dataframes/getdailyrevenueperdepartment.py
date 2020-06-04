from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import sum , round




spark = SparkSession.builder. \
    appName("revenueperdepartment"). \
    master("local"). \
    getOrCreate()


# loaded data into df :
def getdata(args0):
    data = spark.read.format("csv").load(args0)
    return data


# convert df with names:
orders_data = getdata("/home/shashank/pycharm/pyspark/retail_db/orders/part-00000"). \
    toDF("order_id", "order_date", "order_customer_id", "order_status")

orderItems_data = getdata("/home/shashank/pycharm/pyspark/retail_db/order_items/part-00000"). \
    toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity",
         "order_item_subtotal", "order_item_product_price")

products_data = getdata("/home/shashank/pycharm/pyspark/retail_db/products/part-00000"). \
    toDF("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image")

categories_data = getdata("/home/shashank/pycharm/pyspark/retail_db/categories/part-00000"). \
    toDF("category_id", "category_department_id", "category_name")

departments_data = getdata("/home/shashank/pycharm/pyspark/retail_db/departments/part-00000"). \
    toDF("department_id", "department_name")

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

categories = categories_data.withColumn("category_id", categories_data.category_id.cast(IntegerType())). \
    withColumn("category_department_id", categories_data.category_department_id.cast(IntegerType()))

departments = departments_data.withColumn("department_id", departments_data.department_id.cast(IntegerType()))

# joining the datasets:  application using Scala to get daily revenue per department

spark.conf.set('spark.sql.shuffle.partitions', '2')



joined_datasets = orders.join(orderItems, orders.order_id == orderItems.order_item_order_id) \
    .join(products, products.product_id == orderItems.order_item_product_id) \
    .join(categories, categories.category_id == products.product_category_id) \
    .join(departments, departments.department_id == categories.category_department_id) \
    .select(orders.order_date, orderItems.order_item_subtotal, departments.department_id, departments.department_name)

revenue_per_department = joined_datasets. \
    groupBy(joined_datasets.order_date, joined_datasets.department_id, joined_datasets.department_name). \
    agg(round(sum(joined_datasets.order_item_subtotal), 2).alias("revenue_per_department"))











