# Databricks notebook source
# Source discovery: raw cells from one sheet, to find the header row and column types.
bronze_path = "/Volumes/uk_property_intel/bronze/boe/base_rate/baserate.xls"

df_probe = (spark.read
    .format("dev.mauch.spark.excel")
    .option("header", "false")       # see raw cells, including any title block
    .option("inferSchema", "false")  # everything as string for now
    .option("dataAddress", "0!A1")   # sheet by index; "'Raw Data'!A1" addresses by name
    .load(bronze_path))

df_probe.show(15, truncate=False)
print(f"Columns: {len(df_probe.columns)}, Rows: {df_probe.count()}")
