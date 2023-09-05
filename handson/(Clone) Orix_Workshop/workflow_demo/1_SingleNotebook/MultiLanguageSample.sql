-- Databricks notebook source
-- MAGIC %md
-- MAGIC PythonとSQL（並びにR/Scala）を同じノートブックで実行可能

-- COMMAND ----------

-- MAGIC %md
-- MAGIC hoge

-- COMMAND ----------

select 
"This is SQL sample",
"hoge"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("This is Python sample")

-- COMMAND ----------

-- MAGIC %r
-- MAGIC r_sample <- function() {
-- MAGIC   print("This is R sample")
-- MAGIC }
-- MAGIC r_sample()

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC println("This is Scala sample")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import databricks.workspace as workspace_api
-- MAGIC dir(workspace_api)

-- COMMAND ----------


