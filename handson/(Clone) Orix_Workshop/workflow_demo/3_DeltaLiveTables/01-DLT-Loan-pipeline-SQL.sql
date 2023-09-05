-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # デルタライブテーブルでETLを簡素化する
-- MAGIC
-- MAGIC DLTは、データエンジニアリングを誰でも利用できるようにします。SQLまたはPythonで変換を宣言するだけで、DLTがデータエンジニアリングの複雑さを処理してくれます。
-- MAGIC
-- MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-1.png" width="700"/> 
-- MAGIC
-- MAGIC **ETL 開発を加速させる** <br/> <br/>
-- MAGIC シンプルなパイプラインの開発とメンテナンスにより、アナリストやデータエンジニアが迅速にイノベーションを起こせるようになります。
-- MAGIC
-- MAGIC **運用の複雑さを解消** <br/> 
-- MAGIC 複雑な管理作業を自動化し、パイプラインの運用をより広く可視化することで、運用の複雑さを解消します。
-- MAGIC
-- MAGIC **データの信頼性** <br/> <br/>
-- MAGIC 品質管理および品質モニタリングにより、正確で有用なBI、データサイエンス、およびMLを実現します。
-- MAGIC
-- MAGIC **バッチとストリーミングの簡素化** <br/> <br/>
-- MAGIC バッチ処理またはストリーミング処理用の自己最適化および自動スケーリングデータパイプラインを備えています。
-- MAGIC
-- MAGIC ## 私たちのデルタ・ライブ・テーブル・パイプライン
-- MAGIC
-- MAGIC 顧客のローンや過去の取引に関する情報を含む生のデータセットを入力として使用することになります。
-- MAGIC
-- MAGIC 我々の目標は、このデータをほぼリアルタイムで取り込み、データ品質を確保しながらアナリストチームのテーブルを構築することです。
-- MAGIC
-- MAGIC **Your DLT Pipeline is ready! ** あなたのパイプラインはこのノートブックを使って開始され、<a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/26ba724e-24ea-406d-b0a7-5dd29f5b5245">available here</a> です。
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=1444828305810485&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fdata-engineering%2Fdlt-loans%2F01-DLT-Loan-pipeline-SQL&uid=6187853391110359">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC データセットは3つの異なるシステムから提供され、クラウドストレージフォルダ（S3/ADLS/GCS）に保存されています： 
-- MAGIC
-- MAGIC * `loans/raw_transactions` (loans uploader here in every few minutes)
-- MAGIC * `loans/ref_accounting_treatment` (reference table, mostly static)
-- MAGIC * `loans/historical_loans` (loan from legacy system, new data added every week)
-- MAGIC
-- MAGIC このデータを少しずつ取り込み、KPIを報告する最終的なダッシュボードに必要ないくつかの集計を計算してみましょう。

-- COMMAND ----------

-- DBTITLE 1,Let's review the incoming data
-- MAGIC %fs ls /demos/dlt/loans/raw_transactions

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## ブロンズレイヤー：Databricks Autoloaderを活用したデータのインクリメンタルインジェスト
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-2.png" width="600"/> です。
-- MAGIC
-- MAGIC 生データはストレージに送られます。
-- MAGIC
-- MAGIC Autoloaderはこのデータの読み込みを簡素化し、スキーマの推論やスキーマの進化を適用し、何百万もの受信ファイルにも対応できるようにします。
-- MAGIC
-- MAGIC AutoloaderはSQLの`cloud_files`関数で利用でき、様々なフォーマット（json、csv、avro...）で利用することができます：
-- MAGIC
-- MAGIC Autoloaderの詳細については、`dbdemos.install('auto-loader')`を参照してください。
-- MAGIC
-- MAGIC #### ストリーミングライブテーブル 
-- MAGIC テーブルを `STREAMING` として定義することで、新しい受信データのみを消費することが保証されます。STREAMING`がないと、利用可能なすべてのデータを一度にスキャンして取り込むことになります。詳しくは[ドキュメント](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html)を参照してください。

-- COMMAND ----------

-- DBTITLE 1,新規の受信トランザクションをキャプチャする
CREATE STREAMING LIVE TABLE raw_txs_dlt
  COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone"
AS SELECT * FROM cloud_files('/demos/dlt/loans/raw_transactions', 'json', map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,リファレンステーブル - メタデータ（小さい＆ほぼ静的）
CREATE LIVE TABLE ref_accounting_treatment_dlt
  COMMENT "Lookup mapping for accounting codes"
AS SELECT * FROM delta.`/demos/dlt/loans/ref_accounting_treatment`

-- COMMAND ----------

-- DBTITLE 1,レガシーシステムからの履歴トランザクション
-- as this is only refreshed at a weekly basis, we can lower the interval
CREATE STREAMING LIVE TABLE raw_historical_loans_dlt
  TBLPROPERTIES ("pipelines.trigger.interval"="6 hour")
  COMMENT "Raw historical transactions"
AS SELECT * FROM cloud_files('/demos/dlt/loans/historical_loans', 'csv', map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC ## シルバーレイヤー：データ品質を確保しながらテーブルを結合する
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-3.png" width="600"/> 
-- MAGIC
-- MAGIC ブロンズレイヤーが定義されたら、データを結合することでシルバーレイヤーを作成します。ブロンズテーブルは `LIVE` というスペースネームを使用して参照されることに注意してください。
-- MAGIC
-- MAGIC BZ_raw_txs` のようなブロンズレイヤーからのインクリメントのみを消費するために、`stream` keyworkd: `stream(LIVE.BZ_raw_txs)` を使用することになるでしょう。
-- MAGIC
-- MAGIC なお、圧縮についてはDLTが処理してくれるので気にする必要はない。
-- MAGIC
-- MAGIC #### 期待値
-- MAGIC 期待値 (`CONSTRAINT <name> EXPECT <condition>`) を定義することで、データ品質を強制したり追跡したりすることができます。詳しくは[ドキュメント](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html)を参照してください。

-- COMMAND ----------

-- DBTITLE 1,トランザクションをメタデータで充実させる
CREATE STREAMING LIVE VIEW new_txs_dlt
  COMMENT "Livestream of new transactions"
AS SELECT txs.*, ref.accounting_treatment as accounting_treatment FROM stream(LIVE.raw_txs_dlt) txs
  INNER JOIN live.ref_accounting_treatment_dlt ref ON txs.accounting_treatment_id = ref.id

-- COMMAND ----------

-- DBTITLE 1,適切な取引だけを残す。コストセンターが正しくなければ失敗、他は捨てる
CREATE STREAMING LIVE TABLE cleaned_new_txs_dlt (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
  COMMENT "Livestream of new transactions, cleaned and compliant"
AS SELECT * from STREAM(live.new_txs_dlt)

-- COMMAND ----------

-- DBTITLE 1,悪いトランザクションを分離して、さらなる分析を適用
-- This is the inverse condition of the above statement to quarantine incorrect data for further analysis.
CREATE STREAMING LIVE TABLE quarantine_bad_txs_dlt (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date <= date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance <= 0 OR arrears_balance <= 0) ON VIOLATION DROP ROW
)
  COMMENT "Incorrect transactions requiring human analysis"
AS SELECT * from STREAM(live.new_txs_dlt)

-- COMMAND ----------

-- DBTITLE 1,過去のトランザクションを充実させる
CREATE LIVE TABLE historical_txs_dlt
  COMMENT "Historical loan transactions"
AS SELECT l.*, ref.accounting_treatment as accounting_treatment FROM LIVE.raw_historical_loans_dlt l
  INNER JOIN LIVE.ref_accounting_treatment_dlt ref ON l.accounting_treatment_id = ref.id

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## ゴールドレイヤー
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-4.png" width="600"/>
-- MAGIC
-- MAGIC 最後のステップは、ゴールドレイヤーを実体化することです。
-- MAGIC
-- MAGIC これらのテーブルはSQLエンドポイントを使用してスケールで要求されるため、`pipelines.autoOptimize.zOrderCols`を使用してクエリを高速化するためにテーブルレベルでZorderを追加し、残りはDLTが処理します。

-- COMMAND ----------

-- DBTITLE 1,コスト拠点ごとのバランス集計
CREATE LIVE TABLE total_loan_balances_dlt
  COMMENT "Combines historical and new loan data for unified rollup of loan balances"
  TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols" = "location_code")
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM live.historical_txs_dlt  GROUP BY addr_state
  UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.cleaned_new_txs_dlt GROUP BY country_code

-- COMMAND ----------

-- DBTITLE 1,コストセンターごとのバランス集計
CREATE LIVE VIEW new_loan_balances_by_cost_center_dlt
  COMMENT "Live view of new loan balances for consumption by different cost centers"
AS SELECT sum(balance), cost_center_code FROM live.cleaned_new_txs_dlt
  GROUP BY cost_center_code

-- COMMAND ----------

-- DBTITLE 1,国別バランス集計
CREATE LIVE VIEW new_loan_balances_by_country_dlt
  COMMENT "Live view of new loan balances per country"
AS SELECT sum(count), country_code FROM live.cleaned_new_txs_dlt GROUP BY country_code

-- COMMAND ----------

-- MAGIC %md ## 次のステップ
-- MAGIC
-- MAGIC DLTパイプラインの準備が整いましたので、開始します。<a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/26ba724e-24ea-406d-b0a7-5dd29f5b5245">Click here to access the pipeline</a> created for you using this notebook.
-- MAGIC
-- MAGIC 新規に作成する場合は、DLTメニューを開き、パイプラインを作成し、このノートブックを選択して実行します。サンプルデータを生成するには、[コンパニオンノートブック]($./_resources/00-Loan-Data-Generator) を実行してください（データを読み込むパスと書き込むパスが同じであることを確認してください！）。
-- MAGIC
-- MAGIC データアナリストは、DBSQLを使用してデータを分析し、ローン指標を追跡できるようになります。 データサイエンティストもデータにアクセスして、支払い遅延を予測するモデルや、より高度なユースケースの構築を開始することができます。

-- COMMAND ----------

-- MAGIC %md ## Tracking data quality
-- MAGIC
-- MAGIC Expectations stats are automatically available as system table.
-- MAGIC
-- MAGIC This information let you monitor your data ingestion quality. 
-- MAGIC
-- MAGIC You can leverage DBSQL to request these table and build custom alerts based on the metrics your business is tracking.
-- MAGIC
-- MAGIC
-- MAGIC See [how to access your DLT metrics]($./03-Log-Analysis)
-- MAGIC
-- MAGIC <img width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC
-- MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/ab7b8360-2288-46da-8124-2d9b056b6ff2-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard example</a>
