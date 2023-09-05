# Databricks notebook source
# MAGIC %md 
# MAGIC **Requirement**  Databricks Runtime ML & 12.2以上

# COMMAND ----------

# MAGIC %md # Machine Learning End to End Demo 概要
# MAGIC
# MAGIC 実際にRawデータから加工してモデル学習＆デプロイまで構築するデモになります。以下のようなパイプラインを想定しております。
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/overall.png' width='1200'/>

# COMMAND ----------

# MAGIC %md # 01. Create Delta Lake
# MAGIC Azure Blob Storage上のcsvデータを読み込み、必要なETL処理を実施した上でデルタレイクに保存するまでのノートブックになります。
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/1_createDelta.png' width='800' />

# COMMAND ----------

# DBTITLE 1,Setup Script for hands-on
# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md ## Data Load
# MAGIC
# MAGIC DataLake上のデータをロードして、データプロファイルをチェックしてみよう

# COMMAND ----------

# MAGIC %md ### Spark Dataframe による取り込みと加工処理
# MAGIC
# MAGIC [Databricks(Delta lake)のデータ入出力の実装パターン - cheatsheet](https://qiita.com/ktmrmshk/items/54ce2d6f274a67b2e54c)

# COMMAND ----------

# 公開Azure Storage Blobから学習データを取得します (WASBプロトコル)
sourcePath = 'wasbs://public-data@sajpstorage.blob.core.windows.net/customer.csv'
#sourcePath='dbfs:/FileStore/handson/customer.csv'

df_customer_csv = spark.read.format('csv').option("header","true").option("inferSchema", "true").load(sourcePath)
display(df_customer_csv)

# COMMAND ----------

# MAGIC %md ## Delta Lakeに保存
# MAGIC
# MAGIC データサイズが大きくなると処理速度に影響が出るため一度Raw Dataを Delta Lakeに保存することでパフォーマンスを向上させることができます。性能に課題がある場合は、このやり方をお勧めします。

# COMMAND ----------

# save to delta lake
df_customer_csv.write.mode('overwrite').saveAsTable('bronze_table')

# load data as delta
df_bronze = spark.read.table('bronze_table')

# COMMAND ----------

# MAGIC %md ## PySpark Pandas API を使って前処理を実施
# MAGIC
# MAGIC 多くの Data Scientist は、pandasの扱いになれており、Spark Dataframeには不慣れです。Spark 3.2より Pandas APIを一部サポートしました。<br>
# MAGIC これにより、Pandasの関数を使いながら、Sparkの分散機能も使うことが可能になります。 <br>

# COMMAND ----------

import pyspark.pandas as ps

# Convert to koalas
data = df_bronze.pandas_api()

# OHE
data = ps.get_dummies(data, 
                      columns=['gender', 'partner', 'dependents',
                                'phoneService', 'multipleLines', 'internetService',
                                'onlineSecurity', 'onlineBackup', 'deviceProtection',
                                'techSupport', 'streamingTV', 'streamingMovies',
                                'contract', 'paperlessBilling', 'paymentMethod'],dtype = 'int64')

# Convert label to int and rename column
data['churnString'] = data['churnString'].map({'Yes': 1, 'No': 0})
data = data.astype({'churnString': 'int32'})
data = data.rename(columns = {'churnString': 'churn'})
  
# Clean up column names
data.columns = data.columns.str.replace(' ', '')
data.columns = data.columns.str.replace('(', '-', regex=False)
data.columns = data.columns.str.replace(')', '', regex=False)
  
# Drop missing values
data = data.dropna()

# cast the data for totalCharges
data['totalCharges'] = data['totalCharges'].astype(float)
  
data.head(10)

# COMMAND ----------

# MAGIC %md ## Delta Tableとして保存
# MAGIC 現在、UnityCatalogでは Feature Storeをサポートしていないため、通常のDeltaTableを作成します。

# COMMAND ----------

# Pandas Dataframeから Spark DataFrameに変換してから保存
data.to_spark().write.mode('overwrite').saveAsTable('churn_features')

# COMMAND ----------


