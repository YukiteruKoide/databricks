-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## **注** 本コンテンツはハンズオンの対象外です。ログ分析についてお知りになりたい場合には、[dbdemos](https://www.dbdemos.ai/demo.html?demoName=dlt-loans)をご利用ください
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### A cluster has been created for this demo
-- MAGIC To run this demo, just select the cluster `dbdemos-dlt-loans-masataka_yokota` from the dropdown menu ([open cluster configuration](https://e2-demo-field-eng.cloud.databricks.com/#setting/clusters/0405-012517-ry2rf55z/configuration)). <br />
-- MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('dlt-loans')` or re-install the demo: `dbdemos.install('dlt-loans')`*

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # DLT pipeline log analysis
-- MAGIC
-- MAGIC <img style="float:right" width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC
-- MAGIC Each DLT Pipeline saves events and expectations metrics in the Storage Location defined on the pipeline. From this table we can see what is happening and the quality of the data passing through it.
-- MAGIC
-- MAGIC You can leverage the expecations directly as a SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
-- MAGIC
-- MAGIC This notebook extracts and analyses expectation metrics to build such KPIS.
-- MAGIC
-- MAGIC You can find your metrics opening the Settings of your DLT pipeline, under `storage` :
-- MAGIC
-- MAGIC ```
-- MAGIC {
-- MAGIC     ...
-- MAGIC     "name": "test_dlt_cdc",
-- MAGIC     "storage": "/demos/dlt/loans",
-- MAGIC     "target": "quentin_dlt_cdc"
-- MAGIC }
-- MAGIC ```
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=1444828305810485&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fdata-engineering%2Fdlt-loans%2F03-Log-Analysis&uid=6187853391110359">

-- COMMAND ----------

-- DBTITLE 1,Load DLT system table 
-- MAGIC %python
-- MAGIC import re
-- MAGIC current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
-- MAGIC storage_path = '/demos/dlt/loans/'+re.sub("[^A-Za-z0-9]", '_', current_user[:current_user.rfind('@')])
-- MAGIC dbutils.widgets.text('storage_path', storage_path)
-- MAGIC print(f"using storage path: {storage_path}")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC print(current_user)
-- MAGIC print(storage_path)

-- COMMAND ----------

-- MAGIC %python display(dbutils.fs.ls(dbutils.widgets.get('storage_path')))

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/demos/dlt/loans/masataka_yokota/autoloader/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/demos/dlt/loans/masataka_yokota/system/events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pathlib import Path
-- MAGIC
-- MAGIC class DisplayablePath(object):
-- MAGIC     display_filename_prefix_middle = '├──'
-- MAGIC     display_filename_prefix_last = '└──'
-- MAGIC     display_parent_prefix_middle = '    '
-- MAGIC     display_parent_prefix_last = '│   '
-- MAGIC
-- MAGIC     def __init__(self, path, parent_path, is_last):
-- MAGIC         self.path = Path(str(path))
-- MAGIC         self.parent = parent_path
-- MAGIC         self.is_last = is_last
-- MAGIC         if self.parent:
-- MAGIC             self.depth = self.parent.depth + 1
-- MAGIC         else:
-- MAGIC             self.depth = 0
-- MAGIC
-- MAGIC     @property
-- MAGIC     def displayname(self):
-- MAGIC         if self.path.is_dir():
-- MAGIC             return self.path.name + '/'
-- MAGIC         return self.path.name
-- MAGIC
-- MAGIC     @classmethod
-- MAGIC     def make_tree(cls, root, parent=None, is_last=False, criteria=None,max_children=None):
-- MAGIC         root = Path(str(root))
-- MAGIC         criteria = criteria or cls._default_criteria
-- MAGIC
-- MAGIC         displayable_root = cls(root, parent, is_last)
-- MAGIC         yield displayable_root
-- MAGIC
-- MAGIC         children = sorted(list(path
-- MAGIC                                for path in root.iterdir()
-- MAGIC                                if criteria(path)),
-- MAGIC                           key=lambda s: str(s).lower())
-- MAGIC         count = 1
-- MAGIC         for path in children:
-- MAGIC             is_last = count == len(children)
-- MAGIC             if path.is_dir():
-- MAGIC                 yield from cls.make_tree(path,
-- MAGIC                                          parent=displayable_root,
-- MAGIC                                          is_last=is_last,
-- MAGIC                                          criteria=criteria,
-- MAGIC                                          max_children=max_children)
-- MAGIC             else:
-- MAGIC                 yield cls(path, displayable_root, is_last)
-- MAGIC                 if max_children and not is_last and count > max_children:
-- MAGIC                   yield cls(root / "...", displayable_root, is_last)
-- MAGIC                   break
-- MAGIC
-- MAGIC             count += 1
-- MAGIC
-- MAGIC     @classmethod
-- MAGIC     def _default_criteria(cls, path):
-- MAGIC         return True
-- MAGIC
-- MAGIC     @property
-- MAGIC     def displayname(self):
-- MAGIC         if self.path.is_dir():
-- MAGIC             return self.path.name + '/'
-- MAGIC         return self.path.name
-- MAGIC
-- MAGIC     def displayable(self):
-- MAGIC         if self.parent is None:
-- MAGIC             return self.displayname
-- MAGIC
-- MAGIC         _filename_prefix = (self.display_filename_prefix_last
-- MAGIC                             if self.is_last
-- MAGIC                             else self.display_filename_prefix_middle)
-- MAGIC
-- MAGIC         parts = ['{!s} {!s}'.format(_filename_prefix,
-- MAGIC                                     self.displayname)]
-- MAGIC
-- MAGIC         parent = self.parent
-- MAGIC         while parent and parent.parent is not None:
-- MAGIC             parts.append(self.display_parent_prefix_middle
-- MAGIC                          if parent.is_last
-- MAGIC                          else self.display_parent_prefix_last)
-- MAGIC             parent = parent.parent
-- MAGIC
-- MAGIC         return ''.join(reversed(parts))
-- MAGIC paths = DisplayablePath.make_tree(
-- MAGIC     Path('/dbfs/demos/dlt/loans/masataka_yokota/'),
-- MAGIC     max_children=10
-- MAGIC )
-- MAGIC for path in paths:
-- MAGIC     print(path.displayable())

-- COMMAND ----------

-- MAGIC %sh tree /dbfs/demos/dlt/loans/masataka_yokota/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/demos/dlt/loans/masataka_yokota/checkpoints/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/demos/dlt/loans/masataka_yokota/tables/

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW demo_dlt_loans_system_event_log_raw 
-- MAGIC   as SELECT * FROM delta.`$storage_path/system/events`;
-- MAGIC SELECT * FROM demo_dlt_loans_system_event_log_raw order by timestamp desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
-- MAGIC * `user_action` Events occur when taking actions like creating the pipeline
-- MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
-- MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
-- MAGIC   * `flow_type` - whether this is a complete or append flow
-- MAGIC   * `explain_text` - the Spark explain plan
-- MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
-- MAGIC   * `metrics` - currently contains `num_output_rows`
-- MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
-- MAGIC     * `dropped_records`
-- MAGIC     * `expectations`
-- MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
-- MAGIC   

-- COMMAND ----------

-- DBTITLE 1,Lineage Information
SELECT
  details:flow_definition.output_dataset,
  details:flow_definition.input_datasets,
  details:flow_definition.flow_type,
  details:flow_definition.schema,
  details:flow_definition
FROM demo_dlt_loans_system_event_log_raw
WHERE details:flow_definition IS NOT NULL
ORDER BY timestamp

-- COMMAND ----------

-- DBTITLE 1,Data Quality Results
SELECT
  id,
  expectations.dataset,
  expectations.name,
  expectations.failed_records,
  expectations.passed_records
FROM(
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics,
    details:flow_progress.data_quality.dropped_records,
    explode(from_json(details:flow_progress:data_quality:expectations
             ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
  FROM demo_dlt_loans_system_event_log_raw
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your expectations are ready to be queried in SQL! Open the <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/ab7b8360-2288-46da-8124-2d9b056b6ff2-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">data Quality Dashboard example</a> for more details.
