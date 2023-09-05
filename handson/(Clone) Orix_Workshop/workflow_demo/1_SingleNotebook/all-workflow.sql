-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 本ノートブックでの実施事項
-- MAGIC * データの読み込み
-- MAGIC * 読み込んだデータの確認
-- MAGIC * データの加工パイプライン作成

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 使用するカタログ・データベースの設定

-- COMMAND ----------

-- MAGIC %run ../setting

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### データの読み込み

-- COMMAND ----------

-- DBTITLE 1,トランザクションのJSONファイル読み込み
-- MAGIC %sql
-- MAGIC CREATE or replace TEMPORARY VIEW raw_txs_view 
-- MAGIC USING json options(
-- MAGIC   path = "/demos/dlt/loans/raw_transactions/*",
-- MAGIC   inferschema="true"
-- MAGIC );
-- MAGIC create
-- MAGIC or replace table raw_txs COMMENT "Lookup mapping for accounting codes"  as
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   raw_txs_view;

-- COMMAND ----------

-- DBTITLE 1,データ拡張用のテーブル（少量）
-- MAGIC %sql
-- MAGIC CREATE
-- MAGIC or replace table ref_accounting_treatment COMMENT "Lookup mapping for accounting codes" AS
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   delta.`/demos/dlt/loans/ref_accounting_treatment`

-- COMMAND ----------

-- DBTITLE 1,過去のトランザクションデータ（CSV）読み込み(Pyspark)
-- MAGIC %python
-- MAGIC raw_historical_loans = spark.read.format('csv').\
-- MAGIC   options(header='true', inferSchema='true').\
-- MAGIC     load('/demos/dlt/loans/historical_loans/*')
-- MAGIC raw_historical_loans.write.mode("overwrite").saveAsTable("raw_historical_loans")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 読み込んだデータの確認

-- COMMAND ----------

select * from raw_historical_loans limit 1000

-- COMMAND ----------

DESCRIBE HISTORY raw_historical_loans

-- COMMAND ----------

select
  grade,
  count(grade) as count_grade
from
  raw_historical_loans
group by
  grade
order by grade

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### データの加工パイプライン作成

-- COMMAND ----------

-- DBTITLE 1,メタデータを使ってテーブルを充実
create or replace table new_txs 
  COMMENT "Livestream of new transactions"
AS SELECT txs.*, ref.accounting_treatment as accounting_treatment FROM raw_txs txs
  INNER JOIN ref_accounting_treatment ref ON txs.accounting_treatment_id = ref.id

-- COMMAND ----------

-- DBTITLE 1,日付・収支・コストセンサでクレンジング
create
or replace table cleaned_new_txs COMMENT "Livestream of new transactions, cleaned and compliant" AS
SELECT
  *
from
  new_txs
where
  (
    to_date(next_payment_date, "M-dd-yyyy H:mm:ss") > date('2020-12-31')
  )
  and (
    balance > 0
    AND arrears_balance > 0
  )
  and (cost_center_code IS NOT NULL)

-- COMMAND ----------

-- DBTITLE 1,過去の取引データを充実させる
create
or replace table historical_txs COMMENT "Historical loan transactions" AS
SELECT
  l.*,
  ref.accounting_treatment as accounting_treatment
FROM
  raw_historical_loans l 
  INNER JOIN ref_accounting_treatment ref ON l.accounting_treatment_id = ref.id

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

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   new_loan_balances_by_country;

-- COMMAND ----------


