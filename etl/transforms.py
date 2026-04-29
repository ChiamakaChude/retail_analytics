# cleans, joins, logic

df_orders_clean = df_orders.dropDuplicates()


df_orders_clean.printSchema()
print(df_orders_clean.count())