# ADLS RAW file path (copied by pipeline Copy activity)
raw_csv_path = "abfss://salesfilesystem@salesadlsaccount.dfs.core.windows.net/raw/sales/sales.csv"

# Read CSV from ADLS (SUPPORTED)
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_csv_path)
)

# Write as Parquet to RAW zone (normalized format)
raw_parquet_path = "abfss://salesfilesystem@salesadlsaccount.dfs.core.windows.net/raw/sales_parquet"

df_raw.write.mode("overwrite").parquet(raw_parquet_path)

df_raw.show()
