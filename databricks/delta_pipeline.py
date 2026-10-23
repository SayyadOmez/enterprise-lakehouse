# Run inside a Databricks notebook after configuring the catalog/schema and source path.
# Example: spark.conf.set('portfolio.source_path', '/Volumes/my_catalog/my_schema/raw/orders.jsonl')
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

source_path = spark.conf.get('portfolio.source_path')
bronze_table = spark.conf.get('portfolio.bronze_table', 'portfolio.bronze_orders')
silver_table = spark.conf.get('portfolio.silver_table', 'portfolio.silver_orders')
gold_table = spark.conf.get('portfolio.gold_table', 'portfolio.gold_customer_sales')

raw = spark.read.json(source_path).withColumn('ingested_at', F.current_timestamp())
raw.write.format('delta').mode('append').saveAsTable(bronze_table)
valid = (spark.table(bronze_table)
    .filter(F.col('event_id').isNotNull() & F.col('customer_id').isNotNull() & (F.col('amount').cast('double') >= 0))
    .withColumn('amount', F.col('amount').cast('double'))
    .withColumn('updated_at', F.to_timestamp('updated_at')))
w = Window.partitionBy('event_id').orderBy(F.col('updated_at').desc(),F.col('ingested_at').desc())
latest = valid.withColumn('rn', F.row_number().over(w)).filter('rn = 1').drop('rn','ingested_at')
if spark.catalog.tableExists(silver_table):
    (DeltaTable.forName(spark,silver_table).alias('t').merge(latest.alias('s'),'t.event_id = s.event_id')
      .whenMatchedUpdateAll(condition='s.updated_at > t.updated_at').whenNotMatchedInsertAll().execute())
else:
    latest.write.format('delta').saveAsTable(silver_table)
(spark.table(silver_table).groupBy('customer_id').agg(F.sum('amount').alias('total_amount'),F.count('*').alias('order_count'))
    .write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(gold_table))
