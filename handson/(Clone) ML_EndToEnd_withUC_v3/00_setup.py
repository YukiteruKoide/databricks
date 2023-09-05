# Databricks notebook source
# DBTITLE 1,オリジナルのプレフィックスをセットしてください。
# original prefix 
prefix = 'e2eykoide'

# COMMAND ----------

# DBTITLE 1,Unity Catalog設定(管理者はあらかじめ作成し、権限を付与しておいてください）

#管理者は、あらかじめ、ハンズオン用のカタログを作成しておく必要があります。
#メタストア管理者であることを確認の上以下のコマンドを実行して、あらかじめカタログを用意ください。

#---以下を管理者のみ実行---
#spark.sql('CREATE CATALOG IF NOT EXISTS ml_handson')
#spark.sql('GRANT CREATE, USAGE ON CATALOG ml_handson TO `account users`')
#----------------------

# COMMAND ----------

#--- use catalog
spark.sql('USE CATALOG ml_handson')

#--- Create a new schema in the quick_start catalog
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {prefix}_schema')
spark.sql(f'USE {prefix}_schema')

# COMMAND ----------

print(f'prefix: {prefix}')
print(f'Catalog Name: ml_handson')
print(f'Schema Name: {prefix}_schema')
