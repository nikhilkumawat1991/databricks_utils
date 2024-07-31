# Databricks notebook source
# MAGIC %md 
# MAGIC ### ONE TIME SETUP PER ENVIRONMENT.. USED FOR CREATING CONIFER WORKFLOW..

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from utils import *

# COMMAND ----------

# DBTITLE 1,check unity catalog status
utility = utilities.utility()
uc_status = utility.get_uc_status()

# COMMAND ----------

if "not enabled" in uc_status:
    dbutils.notebook.exit("Unity catalog is not enabled. Kindly enable to proceed further..!!")
else:
    print(uc_status)

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# DBTITLE 1,Widgets to get storage account name, access token and cluster name
dbutils.widgets.text("Storage_account","","Storage_account")
dbutils.widgets.text("Databricks_Access_token","","Databricks_Access_token")
dbutils.widgets.text("repo_url","","repo_url")
dbutils.widgets.text("repo_token","","repo_token")
dbutils.widgets.text("cluster_name","","cluster_name")
dbutils.widgets.text("branch_name","","branch_name")
dbutils.widgets.dropdown("customer_name", "conifer", ["conifer", "bevcap", "MDM_workflow"])

# COMMAND ----------

access_token = dbutils.widgets.get("Databricks_Access_token")
storage_name = dbutils.widgets.get("Storage_account")
repo_url = dbutils.widgets.get("repo_url")
repo_token = dbutils.widgets.get("repo_token")
cluster_name = dbutils.widgets.get("cluster_name")
branch_name = dbutils.widgets.get("branch_name")
customer_name = dbutils.widgets.get("customer_name")

# COMMAND ----------

customer_name

# COMMAND ----------

spark.conf.set("spark.databricks.service.client.enabled",False)

# COMMAND ----------

# DBTITLE 1,check whether the token provided in widget is valid or not
token_valid = utility.validate_access_token(access_token)
if token_valid:
    print("Token is valid")
else:
    dbutils.notebook.exit("Invalid access token. Kindly provide valid access before moving forward..!!")


# COMMAND ----------

# DBTITLE 1,Check whether storage account is configured as external location or not
storage_enabled = utility.storage_validation(storage_name)
if storage_enabled:
    print("Storage location configured in External location..!!")
else:
    dbutils.notebook.exit("No external location configured for the given storage. Kindly configure before moving forward..!!")

# COMMAND ----------

spark.conf.set("access_token",access_token)
spark.conf.set("storage_name",storage_name)
spark.conf.set("repo_url",repo_url)
spark.conf.set("repo_token",repo_token)

# COMMAND ----------

# DBTITLE 1,Create secret of the access token
secret_scope = secretScope()

# COMMAND ----------

#access_token = "dapia7625a28e6a3ec08879ae4530f1de9e3-2"

# COMMAND ----------

# DBTITLE 1,Create a databricks secret of provided access token 
cloud = spark.conf.get('spark.databricks.cloudProvider')
secret_scope.create_secret_scope("rest_api",access_token,access_token)

# COMMAND ----------

# DBTITLE 1,Create file trigger job
job_new = job.job_class()

# COMMAND ----------

# DBTITLE 1,Update GIT Token
git_token_response = job_new.set_git_token(repo_token)
print(git_token_response)

# COMMAND ----------

# DBTITLE 1,get cluster_id
if cluster_name:
    status, cluster_id, cluster_state = job_new.check_cluster_exist(cluster_name)
    print(status)
    print(cluster_id)
    print(cluster_state)

# COMMAND ----------

# DBTITLE 1,Start if cluster is terminated
if cluster_name:
    if cluster_state == "TERMINATED":
        job_new.start_cluster(cluster_id)

# COMMAND ----------

if cluster_name == "":
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    print(f"Current cluster id: {cluster_id}")

# COMMAND ----------

# DBTITLE 1,Create a file trigger Workflow and run it on job cluster
cloud = spark.conf.get('spark.databricks.cloudProvider')
try:
    func = getattr(job_new, "create_"+customer_name.lower()+"_workflow")
    print(func)
    job_status = func(customer_name.lower(), cluster_id, repo_url, branch_name)
except AttributeError:
    print("function not found")


# COMMAND ----------

# %sh
# curl --header "Authorization: Bearer dapi9888d215d7b97d2dc75e98e2dca6ae8a-2" --request GET \
# https://adb-4247241537950157.17.azuredatabricks.net/api/2.0/secrets/scopes/list
