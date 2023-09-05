-- Databricks notebook source
-- MAGIC %run ../setting

-- COMMAND ----------

-- DBTITLE 1,コスト拠点ごとの収支集計
CREATE or replace table total_loan_balances
TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols" = "location_code") 
AS
SELECT
  sum(revol_bal) AS bal,
  addr_state AS location_code
FROM
  historical_txs
GROUP BY
  addr_state
UNION
SELECT
  sum(balance) AS bal,
  country_code AS location_code
FROM
  cleaned_new_txs
GROUP BY
  country_code

-- COMMAND ----------

-- DBTITLE 1,国別収支集計 
CREATE
or replace table new_loan_balances_by_country AS
SELECT
  sum(count) sum_result,
  country_code
FROM
  cleaned_new_txs
GROUP BY
  country_code
