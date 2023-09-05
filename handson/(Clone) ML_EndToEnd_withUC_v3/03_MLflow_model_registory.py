# Databricks notebook source
# MAGIC %md ## モデルの登録＆エンドポイントの立ち上げ
# MAGIC
# MAGIC 作成したベストモデルを、MLflowのRegistoryに登録し、stageを`staging`に変更します。<br>
# MAGIC また、リモートから利用出来るようにエンドポイントを立ち上げてモデルサービング機能も有効にします。
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/4_model_endpoint.png' width='800' />

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# 以下のモデル名で登録してください。
print(f'model_name = {prefix}_churn_model')

# COMMAND ----------

# MAGIC %md ### モデルの登録
# MAGIC
# MAGIC 03で学習したモデルを、Experiment UIから登録します。（APIを使った登録も可能です）
# MAGIC
# MAGIC 今回はモデル名を__`{prefix}_churn_model`__とします。
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/model_registory.png' width='800'/>

# COMMAND ----------

# MAGIC %md ##モデルをStagingに変更
# MAGIC
# MAGIC stageの変更をすることによって、クライアント側のコード変更をせずに検証や、本番利用ができるようになります。<br>
# MAGIC Stageは、`staging`, `production`, `archive`の３つです。
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/model_stage.png' width='800' />

# COMMAND ----------

# MAGIC %md ## Model Serving Endpoint の立ち上げ
# MAGIC
# MAGIC RestAPI経由でリモートからモデルを利用したい場合、モデルサービング用のエンドポイントを立ち上げる必要があります。 <br>
# MAGIC 一つ上のレイヤーに移動し、モデルの画面で、`レガシーサービング`というタブを開くとエンドポイントが立ち上がります。
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/handsOn/workshop_quickstart/lagecy_model_serving.png' width='800' />
# MAGIC
# MAGIC 以下のようにStatusがグリーンになるまでお待ちください。<br>
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/handsOn/workshop_quickstart/serving_status.png' width='800' />

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 以上で、モデルのデプロイは終了です。次に RestAPIを使って推論してみましょう。
# MAGIC
# MAGIC また、モデルサービングはデモが終了したら**ストップ**しておくことをお忘れなく。
