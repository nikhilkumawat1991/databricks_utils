# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import *
from utils import *

# COMMAND ----------

utility = utilities.utility()
uc_status = utility.get_uc_status()

# COMMAND ----------

dbutils.widgets.text("Databricks_Access_token","","Databricks_Access_token")

# COMMAND ----------

access_token = dbutils.widgets.get("Databricks_Access_token")

# COMMAND ----------

token_valid = utility.validate_access_token(access_token)
if token_valid:
    print("Token is valid")
else:
    dbutils.notebook.exit("Invalid access token. Kindly provide valid access before moving forward..!!")


# COMMAND ----------

secret_scope = secretScope()


# COMMAND ----------

cloud = spark.conf.get('spark.databricks.cloudProvider')
secret_scope.update_secret_scope("rest_api",access_token,access_token)

# COMMAND ----------


