# Databricks notebook source

# Project configuration
CATALOG_NAME = "traffic_dev"
SCHEMAS = ["bronze", "silver", "gold"]

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

# Create Medallion schemas
for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}")

print(f"Catalog '{CATALOG_NAME}' and schemas {SCHEMAS} created successfully.")
