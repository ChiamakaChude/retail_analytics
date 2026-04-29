# s3 writers (partitioning, formats, etc.)

df_orders_clean.write.mode("overwrite").partitionBy("order_dow").parquet(
    "s3://retail-analytics-store/processed/orders/"
)
