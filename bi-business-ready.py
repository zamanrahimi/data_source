from pyspark.sql.functions import sum as _sum

# Read from CURATED zone (output of curate-data)
curated_parquet_path = "abfss://salesfilesystem@salesadlsaccount.dfs.core.windows.net/curated/sales"

df_curated = spark.read.parquet(curated_parquet_path)

# Aggregate for BI layer
df_bi = (
    df_curated
    .groupBy("state", "category")
    .agg(_sum("amount").alias("total_sales"))
)

# Write to BI zone
bi_path = "abfss://salesfilesystem@salesadlsaccount.dfs.core.windows.net/bi/sales_summary"

df_bi.write.mode("overwrite").parquet(bi_path)

df_bi.show()
