import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re,json,requests
import pandas as pd
from datetime import datetime
import json
import pandas

#Get Existing Spark object from SparkSession
spark = SparkSession.builder.appName('fleming').getOrCreate()

#utility class
class utility:
    def __init__(self):
        #self.dbutils = self.get_dbutils(spark)
        self.conifer_workflow_list = []
        self.cloud = spark.conf.get('spark.databricks.cloudProvider')

    #Function to get existing dbutils object
    def get_db_utils(self,spark):
        dbutils = None
        if spark.conf.get("spark.databricks.service.client.enabled") == "true":
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
            if not dbutils:
                print("could not initialise dbutils!")
        return dbutils

    #Function to extract asOfDate from conifer excel file
    def get_asOfDate(self,file_name, client_name):
        import re
        if client_name == "bevcap":
            date_value = file_name.split("_",1)[0].strip()
        elif client_name == "conifer":
            date_value = file_name.split("-",1)[1].strip()
        date =date_value.split(".")[0].replace(" ", "")
        return date

    #(DEPRECATED) Function to get existing dbutils object
    def get_dbutils(self,spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if not dbutils:
                print("could not initialise dbutils!")
        return dbutils

    #get all external locations configured in workspace
    def get_external_locations(self):
        try:
            uc_details = spark.sql(f"SHOW EXTERNAL LOCATIONS")
            list_ext_location = [{row['name']:row['url']} for row in uc_details.collect()]
            return list_ext_location
        except Exception as e:
            return "error occured while getting external LOCATIONS"
    
    #check whether Unity Catalog is enabled or not
    def get_uc_status(self):
        try:
            uc_details = spark.sql(f"SHOW EXTERNAL LOCATIONS")
            return "enabled"
        except Exception as e:
            if "not enabled" in str(e):
                return "unity catalog is not enabled on this cluster."
            else:
                return "error occured"+str(e)
    
    #get bucket name from a string
    def getBucketName(self,str):
        ch1 = "//"
        ch2 = "/"
        m = re.search(ch1+'(.+?)'+ch2, str)
        if m:
            s2 = m.group(1)
        return s2
    
    #get Workspace URL
    def get_url(self):
        return spark.conf.get("spark.databricks.workspaceUrl")
    
    def get_current_user(self) -> str:
        dbutils = self.get_db_utils(spark)
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    
    #(DEPRECATED) get databricks Hostname
    def get_databricks_host(self):
        import json,requests
        dbutils = self.get_db_utils(spark)
        context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        databricks_host = "https://" + context['tags']['browserHostName']
        return databricks_host

    #get databricks URL
    def get_databricks_url(self):
        import json,requests
        dbutils = self.get_db_utils(spark)
        databricks_host = "https://" + self.get_url()
        return databricks_host

    def get_catalog_name(self, client_name):
        cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
        catalog = ""
        environment = cluster_name.split("-")[1].lower()
        print(f"environment is: {environment}")
        if environment == "dev" or environment == "qa":
            environment = "dev"
        catalog_df = spark.read.table("catalogdetail.default.catalognames")
        catalogName = catalog_df.filter((lower(catalog_df.customer)==client_name) & (lower(catalog_df.environment) == environment)).collect()[0].catalogName
        return catalogName
    
    #function to check whether given storage name is configured as External location in UC or not
    def storage_validation(self,storage_name):
        try:
            if len(storage_name) > 0:
                uc_status = self.get_uc_status()
                if uc_status == "enabled":
                    #check whether external locations are configured or not
                    ext_location = self.get_external_locations()

                    external_url_list = [list(d.values())[0] for d in ext_location]
                    if len(external_url_list) > 0:
                        storage_name = self.getStorageName(storage_name)
                        substr_exist = any(storage_name in url for url in external_url_list)
                        if substr_exist:
                            return True
                        else:
                            return False
                    else:
                        print("No external location configured")
                        return False
                else:
                    print("Unity catalog is not enabled..!!")
                    return False
            else:
                print("Storage name can't be empty..!!")
                return False
        except Exception as e:
            return "error occured"+str(e)
    
    #function to check if access token is valid or not
    def validate_access_token(self,access_token):
        databricks_host = self.get_databricks_url()
        print(f"databricks_host: {databricks_host}")
        url = '{}/api/2.0/clusters/list'.format(databricks_host)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return True
        else:
            return False
    

    #function which will fetch job ID by job Name
    def get_jobId_by_name(self,job_name):
        dbutils = self.get_db_utils(spark)
        headers = {
            "Authorization": f"Bearer {spark.conf.get('access_token')}",
            "Content-Type": "application/json",
        }
        databricks_host = self.get_databricks_url()
        print(f"databricks host: {databricks_host} ")
        url = '{}/api/2.0/jobs/list'.format(databricks_host)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            if not response.json():
                print("No Existing jobs found..!!")
                return "not found"
            job_details = response.json()["jobs"]
            for job in job_details:
                job_id = job["job_id"]
                jobName = job["settings"]["name"]
                if jobName == job_name:
                    return job_id
            else:
                return "not found"
        
    #function to get Job Name by giving Job ID
    def find_job_name_by_id(self,job_list,job_name,headers,databricks_host):
        if job_list:
            for jobId in job_list:
                data = str(json.dumps({"job_id":jobId}))
                url = '{}/api/2.1/jobs/get'.format(databricks_host)
                response = requests.get(url, headers=headers, data=data)
                if response.status_code == 200:
                    job = response.json()["settings"]["name"]
                    if job == job_name:
                        return "found"
            else:
                return "job not found"

    
    #get local repo path in workspace
    def get_repo_path(self):
        try:
            dbutils_new = self.get_db_utils(spark)
            ctx  = json.loads(dbutils_new.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            notebook_path = ctx['extraContext']['notebook_path']
            repo_path = '/'.join(notebook_path.split('/')[:4])
            return repo_path
        except Exception as e:
            print(f"Error occurred while getting repo path: {str(e)}")

    #get Remote Repo path configured
    def get_remote_repo_path(self):
        try:
            dbutils_new = self.get_db_utils(spark)
            notebook_path = dbutils_new.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            repo_path = '/'.join(notebook_path.split('/')[:4])
            return repo_path
        except Exception as e:
            print(f"Error occurred while getting repo path: {str(e)}")

    #Extract Azure Storage name from a given String
    def getStorageName(self,str):
        ch1 = "//"
        ch2 = "/"
        m = re.search(ch1+'(.+?)'+ch2, str)
        if m:
            s2 = m.group(1)
        return s2

    #Remove unwanted spaces from the column names 
    def cleanseColumns(self,df, regex='[^\w]'):
        import re
        cols = []
        for c in df.columns:
            col = re.sub(regex, '', c)
            cols.append(col)
        new = df.toDF(*cols)
        return new
    

    #get file name using dbutils from provided storage account
    def get_csv_from_storage(self,storage_account):
        try:
            dbutils = self.get_db_utils(spark)
            file_names = dbutils.fs.ls(storage_account)
            csv_file = [x.name for x in file_names if x.name.endswith(".csv") or x.name.endswith(".xls") or x.name.endswith(".xlsx")]
            if len(csv_file) <= 0:
                print("No Excel file found")
            else:
                file_name = csv_file[0]
                return file_name
        except Exception as e:
            print(f"error occured while getting file name from storage: {storage_account} and error is: {str(e)}")
            return "error"

    #get current timestamp
    def get_timestamp_as_str(self):
        from datetime import datetime
        timestr = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
        return timestr
    

    #append timestamp to a fileName and move it to Backup folder
    def append_timestamp_to_file(self,file_name,file_extension):
        try:
            print(f"file name is: {file_name}")
            if file_name:
                timestr = self.get_timestamp_as_str()
                target_file_name = file_name.split(".")[0]+"_"+timestr+"."+file_extension
                return target_file_name
            else:
                print("file name can't be empty")
                return ""
        except Exception as e:
            print(f"error occured while appending timestamp to file name: {file_name} and error is: {str(e)}")
            return "error"

    #check whether a given file name exist in a storage account
    def check_csv_exist_in_storage(self,storage_name,file_name):
        try:
            dbutils = self.get_db_utils(spark)
            file_names = dbutils.fs.ls(storage_name)
            csv_file = [x.name for x in file_names if x.name.endswith(".csv") or x.name.endswith(".xls") or x.name.endswith(".xlsx")]
            if file_name in csv_file:
                return True
            else:
                return False
        except Exception as e:
            print(f"error occured while getting file name from storage: {storage_name}")
            return "error"


    #move the conifer excel file to a backup folder once file is read
    def move_csv_to_backup(self,storage_name,file_name,file_extension):
        try:
            dbutils = self.get_db_utils(spark)
            if self.check_csv_exist_in_storage(storage_name,file_name):
                backup_file_name = self.append_timestamp_to_file(file_name,file_extension)
                print(f"backup_file_name is: {backup_file_name}")
                if storage_name.startswith("s3"):
                    dbutils.fs.mv("s3://"+storage_name+"/"+file_name, "s3://"+storage_name+"/backup/"+backup_file_name)
                elif storage_name.startswith("abfss"):
                    dbutils.fs.mv(storage_name+"/"+file_name, storage_name+"/backup/"+backup_file_name)
                    print(f"file --> {file_name} moved to backup folder")
                elif storage_name.startswith("dbfs"):
                    print("storage is a dbfs location..!!")
                    dbutils.fs.mv(storage_name+"/"+file_name, storage_name+"/backup/"+backup_file_name)
                    print(f"file --> {file_name} moved to backup folder")
            else:
                print(f"No such csv file --> {file_name} exists now.! Kindly check the backup folder")
        except Exception as e:
            print(f"error occurred while moving csv file to backup folder..!! {str(e)}")

    #get default date 
    def get_default_date(self):
        from datetime import datetime
        default_date_str = '1970-01-01 00:00:00.000'
        default_date = datetime.strptime(default_date_str, '%Y-%m-%d %H:%M:%S.%f')
        return default_date
    

    #(DEPRECATED) was used in sprint 1-2. Used to get last ETL runtime in SIlver notebook
    def get_Last_Etl_RunDate(self, silver_log_tbl):
        try:
            last_date_df = spark.sql(f"select max(lastExtractDateTime) as last_silver_load from {silver_log_tbl}")
            last_run_date = last_date_df.collect()[0]["last_silver_load"]
            print("last_run_date :",last_run_date)
            if not last_run_date:
                last_run_date = self.get_default_date()
            return last_run_date
        except Exception as e:
            print("error occured: ", str(e))
            return "error"
    

    #get max timestamp from silver table in conifer --> (DEPRECATED as of now)
    def get_max_timestamp(self,silver_table):
        max_tmstmp_df= spark.sql(f"select max(ingestion_timestamp) as timestamp from {silver_table}")
        max_date = max_tmstmp_df.collect()[0]["timestamp"]
        return max_date


    #Udate table with last run date (DEPRECATED as of now)
    def update_last_etl_run_date(self,silver_table,etl_log_tbl):
        max_timestamp = self.get_max_timestamp(silver_table)
        etl_log_status = spark.sql(f"select * from {etl_log_tbl}")
        if etl_log_status.count() > 0:
            print(f"update {etl_log_tbl} set lastExtractDateTime = '{max_timestamp}' where tableName= '{silver_table}'")
            spark.sql(f"update {etl_log_tbl} set lastExtractDateTime = '{max_timestamp}' where tableName= '{silver_table}'")
        else:
            print(f"insert into table {etl_log_tbl} VALUES ('{silver_table}','{max_timestamp}')")
            spark.sql(f"insert into table {etl_log_tbl} VALUES ('{silver_table}','{max_timestamp}')")

    #Find table name using asOfDate in Bronze layer for Origami. 
    def find_table_in_origami(self,asOfDate,tableType, origami_catalog, origami_bronze_schema):
        if tableType.lower() == "claimreviews":
            origami_tbl = spark.sql(f"""SHOW TABLES FROM {origami_catalog}.{origami_bronze_schema} LIKE 'claimreviews*'""")
        elif tableType.lower() == "claimsfull":
            origami_tbl = spark.sql(f"""SHOW TABLES FROM {origami_catalog}.{origami_bronze_schema} LIKE 'claimsfull*'""")
        elif tableType.lower() == "lossevents":
            origami_tbl = spark.sql(f"""SHOW TABLES FROM {origami_catalog}.{origami_bronze_schema} LIKE 'lossevents*'""")
        elif tableType.lower() == " ":
            origami_tbl = spark.sql(f"""SHOW TABLES FROM {origami_catalog}.{origami_bronze_schema} LIKE 'updatevalues*'""")
        if origami_tbl.count() == 0:
            return False
        table_names = origami_tbl.select('tableName').collect()
        tbl_list = [tbl.tableName for tbl in table_names]
        for tbl_name in tbl_list:
            if asOfDate in tbl_name:
                return True
        else:
            return False
    
    #Find table name using asOfDate in Bronze layer for customers.
    def find_table_in_catalog(self,asOfDate,tableType, catalog, bronze_schema, bronze_tbl_type,customer_type):
        print(f"checking the table in existing Bronze database..!!")
        if customer_type == "conifer":
            if tableType.lower() == "detail":
                tbl_df = spark.sql(f"""SHOW TABLES FROM {catalog}.{bronze_schema} LIKE '{bronze_tbl_type}[0-9]*'""")
            elif tableType.lower() == "gross":
                tbl_df = spark.sql(f"""SHOW TABLES FROM {catalog}.{bronze_schema} LIKE '{bronze_tbl_type}[0-9]*'""")
            elif tableType.lower() == "summary":
                tbl_df = spark.sql(f"""SHOW TABLES FROM {catalog}.{bronze_schema} LIKE '{bronze_tbl_type}[0-9]*'""")
        elif customer_type == "bevcap":
            tbl_df = spark.sql(f"""SHOW TABLES FROM {catalog}.{bronze_schema} LIKE '{customer_type}*'""")
        if tbl_df.count() == 0:
            return False
        table_names = tbl_df.select('tableName').collect()
        print(table_names)
        tbl_list = [tbl.tableName for tbl in table_names]
        for tbl_name in tbl_list:
            if asOfDate in tbl_name:
                return True
        else:
            return False


    #get initials from username
    def get_initials(self,user_id):
        if user_id is not None:
            initial = user_id.split(".")[0][0:1]+user_id.split(".")[1].split("@")[0]
            return initial
        else:
            return user_id

    def get_customer_name(self, file_name) -> str:
        if "lpt" in file_name.lower():
            return "conifer"
        elif "bcg" in file_name.lower():
            return "bevcap"
        
    # #workflow log table    
    # def workflowLog(self,workflowName,workflowId,notebookName,status,log,error):
    #     try:
    #         record = { "Workflow_Name":workflowName, "WorkflowId":workflowId, "notebookName": notebookName, "logTime":datetime.now(), "status":status,"logMessage":log, "errorMessage":error}
    #         self.conifer_workflow_list.append(record)
    #     except Exception as e:
    #         print("unable to insert the records to conifer workflow table because of "+str(e))

    def create_workflow_logTable(self, tbl_name, customer_name):
        try:
            spark.sql(f"""CREATE TABLE IF NOT Exists {tbl_name} \
(workflowId BIGINT COMMENT "WorkflowId", \
JobRunId BIGINT COMMENT "Job run Id", \
workflowName STRING COMMENT "workflowName", \
notebookName STRING COMMENT "notebookName", \
jobTriggerTime STRING COMMENT "jobTriggerTime", \
errorMessage STRING COMMENT "errorMessage", \
logMessage STRING COMMENT "logMessage", \
logTime timestamp_ntz NOT NULL COMMENT "logTime", \
status STRING COMMENT "status", \
jobEndTime STRING COMMENT "jobEndTime", \
asOfDate STRING COMMENT "asOfDate" \
) \
using DELTA \
PARTITIONED BY (notebookName) \
COMMENT '{customer_name} workflow log table'""")
            if self.addPrimaryKey(tbl_name, customer_name):
                return f"Table {tbl_name} created successfully..!!" 
            else:
                print(f"Error occurred while adding primary key to table..!")
        except Exception as e:
            print(f"Error occurred while creating table: {tbl_name}")
            return "The error for table: "+str(tbl_name) +": "+ str(e)

    def addPrimaryKey(self,tbl_name, customer_name):
        try:
            spark.sql(f"ALTER TABLE {tbl_name} ADD CONSTRAINT {customer_name}_workflowTbl_pk PRIMARY KEY(logTime);")
            return True
        except Exception as e:
            if "already has a primary key" or "already exists" in str(e):
                print(f"Primary key already exist for {tbl_name}")
                return True
            else:
                print(f"Error occurred while adding primary key for table: {tbl_name} and error is: {str(e)}")
                return False

    # def workflow_log_tbl(self):
    #     workflow_log_df = spark.createDataFrame(self.conifer_workflow_list)
    #     workflow_tbl = "workflow_log_tbl"
    #     workflow_log_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("claimsconifer.silver."+workflow_tbl)

    def insert_into_logtbl(self,workflow_log_tbl_name,job_id,job_name,JobRunId,notebook_name,jobTriggerTime,log_message,error_message,status,jobEndTime,asOfDate):
        spark.sql(f"""insert into {workflow_log_tbl_name} (workflowId,workflowName,JobRunId,notebookName,jobTriggerTime,logMessage,logTime,errorMessage,status,jobEndTime,asOfDate) VALUES ({job_id}, '{job_name}',{JobRunId},'{notebook_name}','{jobTriggerTime}', '{log_message}', now(),'{error_message}', '{status}', '{jobEndTime}','{asOfDate}')""")