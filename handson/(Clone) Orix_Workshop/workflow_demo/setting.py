# Databricks notebook source
USE_CATALOG="field_demos_my" # この値を設定
USE_DATABASE="yukiteru_koide_workflow_loan" # この値を設定　
spark.conf.set("demo_setting.use_catalog", USE_CATALOG)
spark.conf.set("demo_setting.use_database", USE_DATABASE)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog "${demo_setting.use_catalog}";
# MAGIC create database if not exists ${demo_setting.use_database};
# MAGIC use database ${demo_setting.use_database};
