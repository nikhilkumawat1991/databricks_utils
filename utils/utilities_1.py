import pyspark
from pyspark.sql import SparkSession
import re,json,requests
import pandas as pd
from datetime import datetime
from cloudModule import *
import json
import pandas

spark = SparkSession.builder.appName('uc_upgrade').getOrCreate()

class utility:
    def __init__(self):
        self.catalog_schema_dict = self.get_catalog_schema_details()

    def get_catalog_schema_details(self):
        dct= {}
        try:
            catalog_df = spark.sql(f"SHOW CATALOGS") 
            list_of_catalogs = [row['catalog'] for row in catalog_df.collect()]
            for catalog_name in list_of_catalogs:
                catalog_status = spark.sql(f"USE CATALOG {catalog_name}") 
                database_df = spark.sql(f"show schemas")
                list_of_databases = [row['databaseName'] for row in database_df.collect()]
                dct[catalog_name] = list_of_databases
            return dct
        except Exception as e:
            print(f"error occurred while fetching catalog and schema details: {str(e)}")
            return dct

    def create_catalog_schema(self,catalog_name,schema_name):
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

        
    def get_dbutils(self,spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if not dbutils:
                log.warning("could not initialise dbutils!")
        return dbutils

    def get_external_locations(self):
        try:
            uc_details = spark.sql(f"SHOW EXTERNAL LOCATIONS")
            list_ext_location = [{row['name']:row['url']} for row in uc_details.collect()]
            return list_ext_location
        except Exception as e:
            return "error occured while getting external LOCATIONS"
    
    def get_uc_status(self):
        try:
            uc_details = spark.sql(f"SHOW EXTERNAL LOCATIONS")
            return "enabled"
        except Exception as e:
            if "not enabled" in str(e):
                return "unity catalog is not enabled on this cluster."
            else:
                return "error occured"+str(e)
    
    def getBucketName(self,str):
        ch1 = "//"
        ch2 = "/"
        m = re.search(ch1+'(.+?)'+ch2, str)
        if m:
            s2 = m.group(1)
        return s2
    
    
    def convert_df_to_dict(self,pdf):
        sparkDF=spark.createDataFrame(pdf)
        sync_external_table_list = []
        for row in sparkDF.collect():
            destination_table_type = row["Uc_table_type"]
            source_table_type = row["Hive_table_type"]
            if destination_table_type == None or str(destination_table_type).lower() == "nan":
                destination_table_type = source_table_type
            if str(source_table_type).upper() == "EXTERNAL" and str(destination_table_type).upper() == "EXTERNAL":
                sync_external_table_list.append({row['Hive_table_name']:{"location":row["Hive_table_location"],"catalog_name":row["Uc_catalog_name"], "schema_name":row["Uc_schema_name"]}})
        return sync_external_table_list
    
    def convert_df_to_excel(self,sparkDF):
        pandas_df = sparkDF.toPandas()
        excel_file = BytesIO()
        pandas_df.to_excel(excel_file, index=False)

    def get_url(self):
        return spark.conf.get("spark.databricks.workspaceUrl")
    
    
    def get_databricks_host(self):
        import json,requests
        dbutils = self.get_dbutils(spark)
        context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        databricks_host = "https://" + context['tags']['browserHostName']
        return databricks_host
    
    def find_table_in_uc(self,catalog_name,schema_name,tbl_name):
        try:
            tbl_name = tbl_name.split(".")[1]
            tbl_status = spark.sql(f"describe table {catalog_name}.{schema_name}.{tbl_name}")
            return True
        except Exception as e:
            if "not found" in str(e):
                print("table not found in unity catalog...!!!")
                return False
    
    def workflow_dict(self,pdf):
        #{"bronze.customer":{"Hive_table_type":"MANAGED", "Hive_table_format":"delta", "Frequency_of_sync":"Daily","Date_n_time_for_1st_schedule":"16:00:00"}}
        workflow_lst = []
        sparkDF=spark.createDataFrame(pdf)
        for row in sparkDF.collect():
            destination_table_type = row["Uc_table_type"]
            source_table_type = row["Hive_table_type"]
            if destination_table_type == None or str(destination_table_type).lower() == "nan":
                destination_table_type = source_table_type
            if not (str(source_table_type).upper() == "EXTERNAL" and str(destination_table_type).upper() == "EXTERNAL"):
                workflow_lst.append({row['Hive_table_name']:{"Hive_table_type":source_table_type,"Hive_table_format":row["Hive_table_format"],"Sync_required":row["Sync_required"], "Frequency_of_sync":row["Frequency_of_sync"],"Date_n_time_for_1st_schedule":row["Date_n_time_for_1st_schedule"], "Uc_catalog_name":row["Uc_catalog_name"], "Uc_schema_name":row["Uc_schema_name"], "Uc_table_name":row["Uc_table_name"],"Failure_notifications":row["Failure_notifications"] ,"Primary_key_for_cdc":row["Primary_key_for_cdc"],"Uc_table_type":destination_table_type,"Uc_table_format":row["Uc_table_format"],"Uc_external_table_location":row["Uc_external_table_location"]}})
        return workflow_lst
    
    def check_delimiter(self,st):
        if ',' in st:
            return "comma"
        elif ' ' in st:
            return "space"
        else:
            return "nothing"
    
    def datetime_to_cron(self,dt,frequency):
        try:
            frequency = str(frequency).lower()
            del_val = self.check_delimiter(str(dt))
            if del_val == "comma":
                lst =  dt.split(",")
                dt = lst[1].strip()
                prefix = lst[0].strip()
            elif del_val == "space":
                dt =re.sub('\s{2,}', ' ', dt)
                lst = dt.split(" ")
                prefix = lst[0].strip()
                dt = lst[1].strip()
            dt =  datetime.strptime(dt, '%H:%M:%S')
            if frequency == "daily":
                return f"{dt.second} {dt.minute} {dt.hour} * * ? "
            elif frequency == "monthly":
                #"38 52 11 22 * ?"
                return f"{dt.second} {dt.minute} {dt.hour} {prefix} * ? "
            elif frequency == "weekly":
                #"38 52 11 ? * Tue"
                return f"{dt.second} {dt.minute} {dt.hour}  ? * {prefix[0: 3]}"
            elif frequency == "hourly":
                #"38 30 * * * ?"
                return f"{dt.second} {dt.minute} * * *  ? "
            else:
                return "not yet supported"
        except Exception as e:
            print(f"error occurred while converting time to cron expression: {str(e)}")
    
    def read_cloud_excel(self,cloud,filetype):
        dbutils = self.get_dbutils(spark)
        if cloud.lower() == "aws":
            print("cloud provider is AWS")
            aws = awsCloud()
            if dbutils.secrets.get(scope="ktk_uc_storage", key="ktk_uc_storage_secret"):
                bucket_name = dbutils.secrets.get(scope="ktk_uc_storage", key="ktk_uc_storage_secret")
                pdf = aws.read_external_tables(bucket_name,filetype)
                if isinstance(pdf,str):
                    if "error" in pdf:
                        print(pdf)
                return pdf
            else:
                print("storage secret not configured")
        elif cloud.lower() == "gcp":
            from cloudModule import gcpCloud
            gcp = gcpCloud.gcpCloud()
            if dbutils.secrets.get(scope="ktk_uc_storage", key="ktk_uc_storage_secret"):
                bucket_name = dbutils.secrets.get(scope="ktk_uc_storage", key="ktk_uc_storage_secret")
                pdf = gcp.read_external_tables(bucket_name,filetype)
                if isinstance(pdf,str):
                    if "error" in pdf:
                        print(pdf)
                return pdf
            else:
                print("Storage secret not configured")
        elif cloud.lower() == "azure":
            from cloudModule import azureCloud
            azure = azureCloud()
            if dbutils.secrets.get(scope="ktk_uc_storage", key="ktk_uc_storage_secret"):
                bucket_name = dbutils.secrets.get(scope="ktk_uc_storage", key="ktk_uc_storage_secret")
                pdf = azure.read_external_tables(bucket_name,filetype)
                if isinstance(pdf,str):
                    if "error" in pdf:
                        print(pdf)
                return pdf
            else:
                print("Storage secret not configured")

    def storage_validation(self,storage_name):
        try:
            uc_status = self.get_uc_status()
            if uc_status == "enabled":
                #check whether external locations are configured or not
                ext_location = self.get_external_locations()

                external_url_list = [list(d.values())[0] for d in ext_location]
                if len(external_url_list) > 0:
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
        except Exception as e:
            return "error occured"+str(e)
        
    def validate_access_token(self,access_token):
        databricks_host = self.get_databricks_host()
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
        
    
    def check_job_status(self,job_name):
        self.job_list = []
        dbutils = self.get_dbutils(spark)
        headers = {
            "Authorization": f"Bearer {dbutils.secrets.get(scope='ktk_uc_access_token', key='ktk_uc_access_token_secret')}",
            "Content-Type": "application/json",
        }
        databricks_host = self.get_databricks_host()
        url = '{}/api/2.1/jobs/list'.format(databricks_host)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            job_json = response.json()
            if job_json:
                for jobs in job_json["jobs"]:
                    self.job_list.append(jobs["job_id"])
                status = self.find_job_name_by_id(self.job_list,job_name,headers,databricks_host)
                if status == "found":
                    return "found"
                else:
                    return "not found"
            else:
                return "empty"
        else:
            return "not found"
        
    def get_jobId_by_name(self,job_name):
        dbutils = self.get_dbutils(spark)
        headers = {
            "Authorization": f"Bearer {dbutils.secrets.get(scope='ktk_uc_access_token', key='ktk_uc_access_token_secret')}",
            "Content-Type": "application/json",
        }
        databricks_host = self.get_databricks_host()
        url = '{}/api/2.1/jobs/list'.format(databricks_host)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            job_details = response.json()["jobs"]
            for job in job_details:
                job_id = job["job_id"]
                jobName = job["settings"]["name"]
                if jobName == job_name:
                    return job_id
            else:
                return "not found"
        

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
    
    def sync_dry_run(self,row):
        try:
            uc_table = row['Hive_table_name'].split('.')[1]
            syncdry_run_op = spark.sql(f"sync table main.default.{uc_table} from hive_metastore.{row['Hive_table_name']} DRY RUN;")
            lst = [row['description'] for row in syncdry_run_op.collect()]
            return lst[0]
        except Exception as e:
            print(f"Error occured while running dry run for table--> {row['Hive_table_name']}")

    def check_dryRun_status(self,pandas_df):
        pandas_df['dry_run_status'] = pandas_df.apply(lambda row: self.sync_dry_run(row), axis=1)
        return pandas_df
    
    def get_repo_path(self):
        try:
            dbutils_new = self.get_dbutils(spark)
            ctx  = json.loads(dbutils_new.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            notebook_path = ctx['extraContext']['notebook_path']
            repo_path = '/'.join(notebook_path.split('/')[:4])
            return repo_path
        except Exception as e:
            print(f"Error occurred while getting repo path: {str(e)}")

    def update_table_status_in_control_table(self, tbl_name, tbl_status):
        try:
            control_tbl = "uc_upgrade_ktk_catalog.ktk_schema.uc_upgrade_ktk_control_tbl"
            update_status = spark.sql(f"UPDATE {control_tbl} SET Table_Status = {tbl_status} WHERE Hive_table_name = {tbl_name}")
            print(f"Updated control table with table name {tbl_name} and status as {tbl_status}")
        except Exception as e:
            print(f"Error occurred while updating control table status for table {tbl_name}")
            print(str(e))

    def save_df_as_table(self,pandas_df):
        if isinstance(pandas_df,pandas.core.frame.DataFrame):
            pandas_df = spark.createDataFrame(pandas_df)
        pandas_df.write.format("delta").mode("overwrite").saveAsTable("uc_upgrade_ktk_catalog.ktk_schema.uc_upgrade_ktk_control_tbl")
        print("Saved to control table..!!")
