import pyspark,json
from pyspark.sql import SparkSession
from utils import utilities

spark = SparkSession.builder.appName('uc_upgrade').getOrCreate()

class payload:
    def __init__(self):
        pass
    
    def get_json_payload_template(self,cloud):
        if cloud == "gcp":
            return {"name": "","creator_user_name":"","run_as_user_name":"","run_as_owner":"true","tags": {"name": "Koantek_UC_upgrade_tool"},"data_security_mode": "SINGLE_USER","email_notifications": {"on_success": [],"on_failure": [],"no_alert_for_skipped_runs": "false"},"new_cluster": {"spark_version": "12.2.x-cpu-ml-scala2.12","custom_tags": {"ResourceClass": "SingleNode"},"data_security_mode": "SINGLE_USER","node_type_id": "e2-highmem-4","num_workers": 0,"spark_conf": {"spark.master": "local[*, 4]","spark.databricks.cluster.profile": "singleNode"},"runtime_engine": "STANDARD"},"schedule":{"quartz_cron_expression":"","timezone_id":"UTC","pause_status":"UNPAUSED"},"notebook_task": {"notebook_path": "","base_parameters": {"sourceCatalog": "hive_metastore","sourceSchema": "","primaryKey": "","destTable": "","destCatalog": "","destSchema": "","sourceTable": "","ucTableType":"","ucTableFormat":"","ucExternalTableLocation":""}},"max_concurrent_runs": 1}
        elif cloud == "aws":
            return {"name": "","creator_user_name":"","run_as_user_name":"","run_as_owner":"true","tags": {"name": "Koantek_UC_upgrade_tool"} ,"data_security_mode": "SINGLE_USER","email_notifications": {"on_success": [],"on_failure": [],"no_alert_for_skipped_runs": "false"},"new_cluster": {"spark_version": "11.3.x-scala2.12","node_type_id": "m4.large","num_workers": 4, "runtime_engine": "STANDARD", "aws_attributes": {"ebs_volume_type": "GENERAL_PURPOSE_SSD","ebs_volume_count": 1,"ebs_volume_size": 32}},"schedule":{"quartz_cron_expression":"","timezone_id":"UTC","pause_status":"UNPAUSED"},"notebook_task": {"notebook_path": "","base_parameters": {"sourceCatalog": "hive_metastore","sourceSchema":"","primaryKey":"","destTable":"","destCatalog":"","destSchema":"","sourceTable":"","ucTableType":"","ucTableFormat":"","ucExternalTableLocation":""}},"max_concurrent_runs": 1}
        elif cloud == "azure":
            return {"name": "","creator_user_name":"","run_as_user_name":"","run_as_owner":"true","tags": {"name": "Koantek_UC_upgrade_tool"},"data_security_mode": "SINGLE_USER","email_notifications": {"on_success": [],"on_failure": [],"no_alert_for_skipped_runs": "false"},"new_cluster": {"spark_version": "11.2.x-cpu-ml-scala2.12","node_type_id": "Standard_DS3_v2","num_workers": 4,"runtime_engine": "STANDARD"},"schedule":{"quartz_cron_expression":"","timezone_id":"UTC","pause_status":"UNPAUSED"},"notebook_task": {"notebook_path": "","base_parameters": {"sourceCatalog": "hive_metastore","sourceSchema":"","primaryKey":"","destTable":"","destCatalog":"","destSchema":"","sourceTable":"","ucTableType":"","ucTableFormat":"","ucExternalTableLocation":""}},"max_concurrent_runs": 1}


    def create_json_payload_for_api(self,json_payload,tbl_name,tbl_details,type) -> dict:
        try:
            utility = utilities.utility()
            dbutils = utility.get_dbutils(spark)
            repo_path = utility.get_repo_path()
            failure_notification_user = tbl_details["Failure_notifications"]
            if type == "schedule":
                json_payload["schedule"]["quartz_cron_expression"] = utility.datetime_to_cron(tbl_details["Date_n_time_for_1st_schedule"],tbl_details["Frequency_of_sync"])
            elif type == "create":
                removed_key = json_payload.pop('schedule')

        #CDC_UC_sourceTable_destTable
            if tbl_details["Uc_table_name"] is not None and str(tbl_details["Uc_table_name"]).lower() != "nan":
                json_payload["name"] = "CDC_UC_"+tbl_name.split(".")[1]+"_"+tbl_details["Uc_table_name"]
            else:
                json_payload["name"] = "CDC_UC_"+tbl_name.split(".")[1]
            user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["tags"]["user"]
            json_payload["creator_user_name"]= user
            json_payload["run_as_user_name"] = user
            json_payload["email_notifications"]["on_success"] = [user]
            if tbl_details["Failure_notifications"] is not None and str(tbl_details["Failure_notifications"]).lower() != "nan":
                json_payload["email_notifications"]["on_failure"] = [user,failure_notification_user]
            else:
                json_payload["email_notifications"]["on_failure"] = [user]
            
            json_payload["notebook_task"]["notebook_path"] = repo_path+"/Managed_CDF"
            json_payload["notebook_task"]["base_parameters"]["sourceSchema"] = tbl_name.split(".")[0]
            json_payload["notebook_task"]["base_parameters"]["primaryKey"] = tbl_details["Primary_key_for_cdc"]
            if tbl_details["Uc_table_name"] is not None:
                json_payload["notebook_task"]["base_parameters"]["destTable"] = tbl_details["Uc_table_name"]
            else:
                json_payload["notebook_task"]["base_parameters"]["destTable"] = tbl_name.split(".")[1]
            json_payload["notebook_task"]["base_parameters"]["destCatalog"] = tbl_details["Uc_catalog_name"]
            json_payload["notebook_task"]["base_parameters"]["destSchema"] = tbl_details["Uc_schema_name"]
            json_payload["notebook_task"]["base_parameters"]["sourceTable"] = tbl_name.split(".")[1]
            json_payload["notebook_task"]["base_parameters"]["ucTableType"] = tbl_details["Uc_table_type"]
            json_payload["notebook_task"]["base_parameters"]["ucTableFormat"] = tbl_details["Uc_table_format"]
            json_payload["notebook_task"]["base_parameters"]["ucExternalTableLocation"] = tbl_details["Uc_external_table_location"]

            return json_payload

        except Exception as e:
            print(f"error occurred while creating json payload for workflow API: {str(e)}")
