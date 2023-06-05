
How to JOIN Hudi Tables in Incremental fashion with DynamoDB in AWS GLue | Hands on
![Capture](https://github.com/soumilshah1995/How-to-JOIN-Hudi-Tables-in-Incremental-fashion-with-DynamoDB-in-AWS-GLue-Hands-on/assets/39345855/32c0abcd-3226-41df-9115-ce07afe3bec3)

# How To Use template 
![image](https://github.com/soumilshah1995/How-to-JOIN-Hudi-Tables-in-Incremental-fashion-with-DynamoDB-in-AWS-GLue-Hands-on/assets/39345855/0bc84efb-9033-44cd-9302-29d08e2bd2a7)

# Steps to follow 

### Step 1 Create DynamoDB table called customers 
![image](https://github.com/soumilshah1995/How-to-JOIN-Hudi-Tables-in-Incremental-fashion-with-DynamoDB-in-AWS-GLue-Hands-on/assets/39345855/16d3ab61-2569-47a1-989b-afdc80de7574)
![image](https://github.com/soumilshah1995/How-to-JOIN-Hudi-Tables-in-Incremental-fashion-with-DynamoDB-in-AWS-GLue-Hands-on/assets/39345855/20712a81-f7d8-4b72-bc19-abc1a57e0397)


### Step 2 Create Sample Glue job create_hudi_table.py on AWS Glue 
* This will create a Hudi table called orders and populate dynamodb table 'customers' with some fake data 
![image](https://github.com/soumilshah1995/How-to-JOIN-Hudi-Tables-in-Incremental-fashion-with-DynamoDB-in-AWS-GLue-Hands-on/assets/39345855/693bdd9a-d908-4caf-a161-f7684447220a)
```
--additional-python-modules  | faker==11.3.0,pynamodb
--conf  |  spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
--datalake-formats | hudi
```

### Step 3 Run Template which will fetch data from dynamodb and will fetch the data from Hudi in incremental Fashion incremental_etl.py and check the cloudwatch as shown in videos

# Enjoy
