import requests, json
from utils import utilities
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType 


spark = SparkSession.builder.appName('uc_upgrade').getOrCreate()
utility = utilities.utility()
dbutils = utility.get_dbutils(spark)

class workflow:
    def __init__(self):
        self.schema = StructType().add("jobId",StringType(),True).add("jobName",StringType(),True).add("table_name",StringType(),True).add("destination_catalog_name",StringType(),True).add("destination_schema_name",StringType(),True).add("status",StringType(),True).add("log_message",StringType(),True).add("log_time",TimestampType(),True) 
        self.workflow_list = []
        # Define the headers for the API request
        self.headers = {
            "Authorization": f"Bearer {dbutils.secrets.get(scope='ktk_uc_access_token', key='ktk_uc_access_token_secret')}",
            "Content-Type": "application/json",
        }

    def create_and_schedule_job(self, job_payload):
        try:
            job_id = ""
            # Create a new job
            utility = utilities.utility()
            url = utility.get_databricks_host()
            response = requests.post(
                f"{url}/api/2.1/jobs/create", headers=self.headers, data=job_payload
            )
            if response.status_code == 200:
                job_id = response.json()["job_id"]
            return response.status_code,str(job_id)
        except Exception as e:
            return "error occured while creating job " + str(e)

    def iterate_managed_tables(self,payload,lst,cloud):
        if lst:
            for tbl_details in lst:
                #Get cloud specific json payload
                json_payload = payload.get_json_payload_template(cloud.lower())
                # print(f"payload is {json_payload}")
                tbl_name = list(tbl_details.keys())[0]
                catalog_name = tbl_details[tbl_name]["Uc_catalog_name"]
                schema_name = tbl_details[tbl_name]["Uc_schema_name"]
                sync_required = tbl_details[tbl_name]["Sync_required"]
                try:
                    #check if sync required option is yes or no
                    if sync_required == "yes":
                        print(f"Sync_required parameter is yes for table: {tbl_name} ")
                        # check if the table format is delta and schedule frequency and date time is not None
                        if (tbl_details[tbl_name]["Frequency_of_sync"] or tbl_details[tbl_name]["Date_n_time_for_1st_schedule"] ) != None and (tbl_details[tbl_name]["Frequency_of_sync"].lower() or tbl_details[tbl_name]["Date_n_time_for_1st_schedule"].lower() )  != 'nan':
                            Frequency_of_sync = tbl_details[tbl_name]["Frequency_of_sync"]
                            Date_n_time_for_1st_schedule = tbl_details[tbl_name]["Date_n_time_for_1st_schedule"]
                            print(f"checking and scheduling workflow for table name: {tbl_name}.!! Frequency is {Frequency_of_sync} and schedule time is {Date_n_time_for_1st_schedule}")
                            
                            # create payload for individual managed tables to be used by REST API
                            json_payload = payload.create_json_payload_for_api(
                                json_payload, tbl_name, tbl_details[tbl_name], "schedule"
                            )
                        else:
                            print(f"Frequency or time of for table {tbl_name} is empty..!! So just creating the workflow..!!")
                            # create payload for individual managed tables to be used by REST API
                            json_payload = payload.create_json_payload_for_api(
                                json_payload, tbl_name, tbl_details[tbl_name], "create"
                            )

                        job_name = json_payload["name"]
                        print(f"job name is: {job_name}")
                        job_status = utility.check_job_status(job_name)
                        print(f"job exist status: {job_status}")
                        json_payload = json.dumps(json_payload)
                        job_payload = str(json_payload)
                        #check if job is already created or not
                        if job_status == "not found":
                            schedule_out,jobId = self.create_and_schedule_job(job_payload)
                            if "error" in str(schedule_out):
                                self.syncWorkflowLog("N/A","N/A",tbl_name,catalog_name,schema_name,"Failed",str(schedule_out))
                            else:
                                print(f"job {job_name} scheduled for {tbl_name} and status is {schedule_out}")
                                self.syncWorkflowLog(jobId,job_name,tbl_name,catalog_name,schema_name,"Success","None")
                        else:
                            job_id = utility.get_jobId_by_name(job_name)
                            print(f"job --> {job_name} already created and scheduled..!!")
                            self.syncWorkflowLog(job_id,job_name,tbl_name,catalog_name,schema_name,"Skipped","job already scheduled")
                    else:
                        print(f"skipping the workflow creation for table: {tbl_name} as sync_required parameter is false")
                        self.syncWorkflowLog("N/a","N/A",tbl_name,catalog_name,schema_name,"Skipped","sync_required parameter is NO")
                except Exception as e:
                    print(f"Error occured while scheduling job..!! {str(e)}")
                    self.syncWorkflowLog("N/A","N/A",tbl_name,catalog_name,schema_name,"Failed",str(e))
        else:
            print(f"No managed tables found..!!")


    def syncWorkflowLog(self,jobId,job_name,tblname,catalog_name,schema_name,status,error):
        try:
            record = {"jobId":jobId,"jobName":job_name,"table_name":tblname, "destination_catalog_name":catalog_name, "destination_schema_name":schema_name, "status":status,"log_message":error,"logTime":datetime.now()}
            self.workflow_list.append(record)
        except Exception as e:
            print("unable to insert the records to workflow log table because of "+str(e))
            
            
    def uc_workflow_sync_log_tbl(self):
        workflow_sync_log_df = spark.createDataFrame(self.workflow_list,self.schema)
        #etl_tbl = "uc_sync_log_tbl"
        workflow_sync_log_df.write.format("delta").mode("append").saveAsTable("uc_upgrade_ktk_catalog.ktk_schema.uc_workflow_log_tbl")