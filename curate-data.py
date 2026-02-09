from pyspark.sql.functions import col, year, to_date

# Read from RAW PARQUET (output of load-raw-data)
raw_parquet_path = "abfss://salesfilesystem@salesadlsaccount.dfs.core.windows.net/raw/sales_parquet"

df_raw = spark.read.parquet(raw_parquet_path)

# Clean + curate data
df_curated = (
    df_raw
    .dropna(subset=["amount"])                 # remove bad rows
    .withColumn("order_dt", to_date(col("order_date")))
    .drop("order_date")
    .withColumn("order_year", year(col("order_dt")))
)

# Write to CURATED zone
curated_path = "abfss://salesfilesystem@salesadlsaccount.dfs.core.windows.net/curated/sales"

df_curated.write.mode("overwrite").parquet(curated_path)

df_curated.show()
