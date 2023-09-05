# Databricks notebook source
# MAGIC %run ../setting

# COMMAND ----------

# DBTITLE 1,トランザクションのJSONファイル読み込み
# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY VIEW raw_txs_view USING json options(
# MAGIC   path = "/demos/dlt/loans/raw_transactions/*",
# MAGIC   inferschema="true"
# MAGIC );
# MAGIC create
# MAGIC or replace table raw_txs COMMENT "Lookup mapping for accounting codes"  as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   raw_txs_view;

# COMMAND ----------

# DBTITLE 1,データ拡張用のテーブル（少量）
# MAGIC %sql
# MAGIC CREATE
# MAGIC or replace table ref_accounting_treatment COMMENT "Lookup mapping for accounting codes" AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta.`/demos/dlt/loans/ref_accounting_treatment`

# COMMAND ----------

# DBTITLE 1,過去のトランザクションデータ（CSV）読み込み
# MAGIC %sql
# MAGIC CREATE
# MAGIC or replace TEMPORARY VIEW raw_historical_loans_view USING CSV options(
# MAGIC   path = "/demos/dlt/loans/historical_loans/*",
# MAGIC   header = "true",
# MAGIC   inferschema="true"
# MAGIC );
# MAGIC create
# MAGIC or replace table raw_historical_loans COMMENT "Raw historical transactions" as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   raw_historical_loans_view;
