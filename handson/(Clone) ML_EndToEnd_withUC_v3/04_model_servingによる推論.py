# Databricks notebook source
# MAGIC %md # モデルサービングによる推論
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/5_model_serving.png' width='800' />

# COMMAND ----------

# MAGIC %md ## トークンの発行
# MAGIC
# MAGIC モデルサービング機能を使って、推論するためには、トークンを発行して置く必要があります。　<br>
# MAGIC `Setting` - `User Settings` - `Access Tokens`
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/token.png' width='800' />

# COMMAND ----------

# MAGIC %md ## Curl によるアクセス
# MAGIC
# MAGIC サンプルデータとしてこちらの`data.json`をローカルにダウンロードする (右クリックで保存)
# MAGIC
# MAGIC https://sajpstorage.blob.core.windows.net/public-data/mldata.json

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Model Servingのページから　Serving URLをコピーして、以下のサンプルのように実行します。
# MAGIC
# MAGIC ```
# MAGIC curl \
# MAGIC  -u token:$DATABRICKS_TOKEN \
# MAGIC  -X POST \
# MAGIC  -H 'Content-Type: application/json' \
# MAGIC  -d@mldata.json \
# MAGIC  $DATABRICKS_MODEL_SERVING_URL
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/curl2.png' width='800' /> <br>
# MAGIC (*) sudo 実行が必要な場合があります。

# COMMAND ----------

# MAGIC %md ## Python　コードによるクライアントアクセス
# MAGIC
# MAGIC Pythonコードでも、エンドポイント経由でアクセスする事ができます。<br>
# MAGIC
# MAGIC 以下は、クライアント側のコードで実行しているイメージでご覧ください。

# COMMAND ----------

import os

# Token をコピペ
os.environ["DATABRICKS_TOKEN"] = 'dapi4175a6f40b2b9fbddde32d87cc85743f'
os.environ["MODEL_URL"] = 'https://adb-984752964297111.11.azuredatabricks.net/model/e2ejmaru_churn_model/Staging/invocations'

# Secretを利用したトークンの引き渡しの場合
#os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get("maru_scope", "token")

# COMMAND ----------

# DBTITLE 1,score_model 関数の定義

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = os.environ.get("MODEL_URL")
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

# DBTITLE 1,Sample Dataの取得
# モデルサービングは、比較的小さいデータバッチにおいて低レーテンシーで予測するように設計されています。

sample_data = 'wasbs://public-data@sajpstorage.blob.core.windows.net/ml_sample.csv'
sample_df = spark.read.format('csv').option("header","true").option("inferSchema", "true").load(sample_data).toPandas()
display(sample_df)

# COMMAND ----------

# DBTITLE 1,Model Servingを使った予測結果
served_predictions = score_model(sample_df)
print(served_predictions)

# COMMAND ----------


