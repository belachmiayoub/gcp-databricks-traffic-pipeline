# Databricks notebook source
# Creates the Unity Catalog catalog and Medallion schemas for the selected environment.

dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env").strip().lower()

if env not in {"dev", "uat", "prd"}:
    raise ValueError("env must be one of: dev, uat, prd")

catalog = f"{env}_catalog"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"Ready: {catalog}.bronze, {catalog}.silver, {catalog}.gold")
