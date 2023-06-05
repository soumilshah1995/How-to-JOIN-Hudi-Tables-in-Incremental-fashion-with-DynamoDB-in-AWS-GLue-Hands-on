try:
    import sys, random, uuid
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from datetime import datetime, date
    import boto3, pandas
    from functools import reduce
    from pyspark.sql import Row
    from faker import Faker
except Exception as e:
    print("Modules are missing : {} ".format(e))

job_start_ts = datetime.now()
ts_format = '%Y-%m-%d %H:%M:%S'

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()

global faker
faker = Faker()


def get_customer_data(total_customers=2):
    customers_array = []
    for i in range(0, total_customers):
        customer_data = {
            "customer_id": str(uuid.uuid4()),
            "name": faker.name(),
            "state": faker.state(),
            "city": faker.city(),
            "email": faker.email(),
            "created_at": datetime.now().isoformat().__str__(),
            "address": faker.address(),

        }
        customers_array.append(customer_data)
    return customers_array


def get_orders_data(customer_ids, order_data_sample_size=3):
    orders_array = []
    for i in range(0, order_data_sample_size):
        try:
            order_id = uuid.uuid4().__str__()
            customer_id = random.choice(customer_ids)
            order_data = {
                "order_id": order_id,
                "name": faker.text(max_nb_chars=20),
                "order_value": random.randint(10, 1000).__str__(),
                "priority": random.choice(["LOW", "MEDIUM", "HIGH"]),
                "order_date": faker.date_between(start_date='-30d', end_date='today').strftime('%Y-%m-%d'),
                "customer_id": customer_id,

            }
            orders_array.append(order_data)
        except Exception as e:
            print(e)
    return orders_array


global total_customers, order_data_sample_size

total_customers = 50
order_data_sample_size = 100

# ---------------------------- CUSTOMERS ------------------------------------------------
customer_data = get_customer_data(total_customers=total_customers)

spark_df_customers = spark.createDataFrame(data=[tuple(i.values()) for i in customer_data],
                                           schema=list(customer_data[0].keys()))
spark_df_customers.show()

# ------------------------------ ORDERS----------------------------------------------------
order_data = get_orders_data(
    order_data_sample_size=order_data_sample_size,
    customer_ids=[i.get("customer_id") for i in customer_data]
)

spark_df_orders = spark.createDataFrame(data=[tuple(i.values()) for i in order_data], schema=list(order_data[0].keys()))
spark_df_orders.show()
# ============================== Settings =======================================
db_name = "hudidb"
table_name = "orders"
recordkey = 'order_id'
precombine = "order_id"
PARTITION_FIELD = 'priority'
BUCKET = "soumilshah-hudi-demos"
path = f"s3://{BUCKET}/silver/table_name={table_name}"
method = 'bulk_insert'
table_type = "COPY_ON_WRITE"
# =================================WRITE INTO HUDI==========================================
hudi_part_write_config = {
    'className': 'org.apache.hudi',

    'hoodie.table.name': table_name,
    'hoodie.datasource.write.table.type': table_type,
    'hoodie.datasource.write.operation': method,
    'hoodie.bulkinsert.sort.mode': "NONE",
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.precombine.field': precombine,

    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.support_timestamp': 'false',
    'hoodie.datasource.hive_sync.database': db_name,
    'hoodie.datasource.hive_sync.table': table_name,

}
spark_df_orders.write.format("hudi").options(**hudi_part_write_config).mode("append").save(path)
# ==================================================
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute

class CustomerModel(Model):
    class Meta:
        table_name = 'customers'
        region = 'us-east-1'  # Replace with your desired AWS region

    customer_id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute()
    state = UnicodeAttribute()
    city = UnicodeAttribute()
    email = UnicodeAttribute()
    created_at = UnicodeAttribute()
    address = UnicodeAttribute()


pandas_df = spark_df_customers.toPandas()
column_names = pandas_df.columns.tolist()

for _, row in pandas_df.iterrows():
    customer_id = row['customer_id']
    name = str(row['name'])
    state = str(row['state'])
    city = str(row['city'])
    email = str(row['email'])
    created_at = str(row['created_at'])
    address = str(row['address'])

    customer = CustomerModel(
        customer_id=customer_id,
        name=name,
        state=state,
        city=city,
        email=email,
        created_at=created_at,
        address=address
    )
    customer.save()
