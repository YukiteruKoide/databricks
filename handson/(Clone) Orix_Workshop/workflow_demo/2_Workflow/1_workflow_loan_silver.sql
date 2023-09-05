-- Databricks notebook source
-- MAGIC %run ../setting

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

create or replace table cleaned_new_txs 
  COMMENT "Livestream of new transactions, cleaned and compliant"
AS SELECT * from new_txs
where (next_payment_date <= date('2020-12-31')) or  (balance <= 0 OR arrears_balance <= 0)

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
