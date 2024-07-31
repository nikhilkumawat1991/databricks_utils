import requests,json
from utils import utilities
from pyspark.sql import SparkSession

#get existing spark object using sparkSession
spark = SparkSession.builder.appName('fleming_file_trigger').getOrCreate()


#Generic job class used to create workflow for conifer as of now.
class job_class:
    def __init__(self):
        self.utility = utilities.utility()

    #start the given cluster_id cluster
    def start_cluster(self,cluster_id):
        try:
            dbutils = self.utility.get_db_utils(spark)
            headers = {
            "Authorization": f"Bearer {spark.conf.get('access_token')}",
            "Content-Type": "application/json",
            }
            data = str(json.dumps({"cluster_id":cluster_id}))
            # Start Terminated Cluster
            url = self.utility.get_databricks_url()
            response = requests.post(
                f"{url}/api/2.0/clusters/start", headers=headers, data=data
            )
            if response.status_code == 200:
                if not response.json():
                    return response.status_code,"Staring"
                else:
                    error_response = response.json()
                    print(f"Error occurred while starting cluster...!!")
                    return "error", error_response["message"]
        except Exception as e:
            return "error occured while starting cluster " + str(e)

    #start the given cluster_id cluster
    def set_git_token(self,git_token):
        try:
            dbutils = self.utility.get_db_utils(spark)
            headers = {
            "Authorization": f"Bearer {spark.conf.get('access_token')}",
            "Content-Type": "application/json",
            }
            url = self.utility.get_databricks_url()
            username = self.utility.get_current_user()
            print(f"username is: {username}")
            data = str(json.dumps({"personal_access_token":git_token, "git_username": username,"git_provider": "azureDevOpsServices" }))
            response = requests.post(
                f"{url}/api/2.0/git-credentials", headers=headers, data=data
            )
            print(f"response is: {response.json()}  and status code is: {response.status_code}")
            if response.status_code == 200:
                if not response.json():
                    return response.status_code
                else:
                    api_response = response.json()
                    print(api_response)
                    return api_response
            elif response.status_code == 400:
                cred_id = self.get_git_token()
                print(f"existing cred id found : {cred_id}")
                if cred_id:
                    update_token_status = self.update_git_token(git_token, cred_id)
                    if update_token_status:
                        print(f"Git token updated..!!")
                    else:
                        print(f"Git token updation failed..!!")
        except Exception as e:
            return "error occured while setting git token" + str(e)

    #start the given cluster_id cluster
    def get_git_token(self):
        try:
            dbutils = self.utility.get_db_utils(spark)
            headers = {
            "Authorization": f"Bearer {spark.conf.get('access_token')}",
            "Content-Type": "application/json",
            }
            url = self.utility.get_databricks_url()
            username = self.utility.get_current_user()
            response = requests.get(
                f"{url}/api/2.0/git-credentials", headers=headers
            )
            if response.status_code == 200:
                if not response.json():
                    return response.status_code
                else:
                    #{'credentials': [{'credential_id': 740650268692505, 'git_provider': 'azureDevOpsServices', 'git_username': 'nikhil.kumawat@flemingih.com'}]}
                    api_response = response.json()
                    for cred_details in api_response["credentials"]:
                        if username in cred_details["git_username"]:
                            return cred_details["credential_id"]
                    else:
                        print("No details found.")
                    return True
            elif response.status_code == 400:
                return False
        except Exception as e:
            print(f"error occured while getting git token {str(e)}" )
            return False
        

    def update_git_token(self,git_token,cred_id):
        try:
            dbutils = self.utility.get_db_utils(spark)
            headers = {
            "Authorization": f"Bearer {spark.conf.get('access_token')}",
            "Content-Type": "application/json",
            }
            url = self.utility.get_databricks_url()
            username = self.utility.get_current_user()
            data = str(json.dumps({"personal_access_token":git_token, "git_username": username,"git_provider": "azureDevOpsServices" }))
            response = requests.patch(
                f"{url}/api/2.0/git-credentials/{cred_id}", headers=headers, data=data
            )
            print(f"response from update is: {response.json()}  and status code is: {response.status_code}")
            if response.status_code == 200:
                if not response.json():
                    return response.status_code
                else:
                    api_response = response.json()
                    return True
            elif response.status_code == 400:
                return False
        except Exception as e:
            print(f"error occured while setting git token {str(e)}" )
            return False
        
        

    #function to check whether the given cluster exist or not
    def check_cluster_exist(self, cluster_name):
        try:
            cluster_dict = {}
            dbutils = self.utility.get_db_utils(spark)
            headers = {
            "Authorization": f"Bearer {spark.conf.get('access_token')}",
            "Content-Type": "application/json",
            }
            # List all clusters detail
            url = self.utility.get_databricks_url()
            response = requests.get(
                f"{url}/api/2.0/clusters/list", headers=headers
            )
            if response.status_code == 200:
                try:
                    cluster_res = response.json()["clusters"]
                    if len(cluster_res) > 0:
                        for cluster_detail in cluster_res:
                            cluster_dict[cluster_detail["cluster_name"]] = {"cluster_id":cluster_detail["cluster_id"], "cluster_state": cluster_detail["state"]}
                    else:
                        print("No existing custers found..!!")
                except Exception as e:
                    print(f"Error occured. {str(e)}")
                if cluster_name in cluster_dict.keys():
                    return cluster_name+" found in existing list..!!", cluster_dict[cluster_name]["cluster_id"], cluster_dict[cluster_name]["cluster_state"]
                else:
                    return cluster_name+" Not found..!!",None, None
        except Exception as e:
            return "error occured while listing clusters " + str(e)

    #create conifer workflow. This function is used in "conifer one time setup" script
    def create_conifer_workflow(self,customer_name,cluster_id,repo_url, branch_name):
        try:
            job_name = customer_name+"_workflow"
            job_status = self.utility.get_jobId_by_name(job_name)
            print(f"job status is: {job_status}")
            if job_status == "not found":
                json_payload = self.get_json_payload_template("conifer","conifer_multi_task")
                #Add job name
                json_payload["name"] = job_name
                #Add repo URL
                json_payload["git_source"]["git_url"] = repo_url
                #Add branch name
                if branch_name:
                    json_payload["git_source"]["git_branch"] = branch_name
                #Trigger the job using file arrival method
                json_payload["trigger"]["file_arrival"]["url"] = spark.conf.get('storage_name')
                #repo_path = self.utility.get_remote_repo_path()
                for task_num in range(0,len(json_payload["tasks"])):
                    #add existing cluster id
                    json_payload["tasks"][task_num]["existing_cluster_id"] = cluster_id
                    if "base_parameters" in json_payload["tasks"][task_num]["notebook_task"].keys(): 
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["storage_location"] = spark.conf.get('storage_name')
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["job_id"] = '{{job.id}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["job_name"] = '{{job.name}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["start_date"] = '{{start_date}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["start_time"] = '{{job.trigger.time.[iso_datetime]}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["run_id"] = '{{job.run_id}}'
                    #Add the repo name
                    if "conifer_control" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/bronze_fleming_control"
                    elif "conifer_detail" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/bronze_fleming_detail"
                    elif "conifer_gross" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/bronze_fleming_gross"
                    elif "conifer_summary" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/bronze_fleming_summary"
                    elif "excel_to_backup" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/excel_file_backup"
                    elif "detail_Aggregation" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/conifer_Detail_Aggregation"
                    elif "month_by_month_differential" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/conifer_month_by_month_differential"
                    elif "Silver_Aggregation" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "conifer/silver_table_aggregation"
                print(f"final payload before job creation: {json_payload}")
                status_code,job_id = self.create_workflow(json_payload)
                if status_code == "error":
                    print(f"Error occured while creating job: {job_id}")
                else:
                    print(f"{job_name} job created successfully..!! and job Id is: {job_id}")
            else:
                print(f"Job --> {job_name} is already created..!!")
        except Exception as e:
            print(f"Error occurred while creating job {job_name}..!! {str(e)}")

    def create_bevcap_workflow(self,customer_name,cluster_id,repo_url, branch_name):
        try:
            job_name = customer_name+"_workflow"
            job_status = self.utility.get_jobId_by_name(job_name)
            print(f"job status is: {job_status}")
            if job_status == "not found":
                json_payload = self.get_json_payload_template("bevcap","bevcap_multi_task")
                #Add job name
                json_payload["name"] = job_name
                #Add repo URL
                json_payload["git_source"]["git_url"] = repo_url
                #Add branch name
                if branch_name:
                    json_payload["git_source"]["git_branch"] = branch_name
                #Trigger the job using file arrival method
                json_payload["trigger"]["file_arrival"]["url"] = spark.conf.get('storage_name')
                #repo_path = self.utility.get_remote_repo_path()
                for task_num in range(0,len(json_payload["tasks"])):
                    #add existing cluster id
                    json_payload["tasks"][task_num]["existing_cluster_id"] = cluster_id
                    if "base_parameters" in json_payload["tasks"][task_num]["notebook_task"].keys(): 
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["storage_location"] = spark.conf.get('storage_name')
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["job_id"] = '{{job.id}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["job_name"] = '{{job.name}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["start_date"] = '{{start_date}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["start_time"] = '{{job.trigger.time.[iso_datetime]}}'
                        json_payload["tasks"][task_num]["notebook_task"]["base_parameters"]["run_id"] = '{{job.run_id}}'
                    #Add the repo name
                    if "bevcap_bronze" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "bevcap/bevcap_bronze"
                    elif "bevcap_goldReporting" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "bevcap/bevcap_silver"
                    elif "excel_to_backup" in json_payload["tasks"][task_num]["task_key"]:
                        json_payload["tasks"][task_num]["notebook_task"]["notebook_path"] = "bevcap/excel_file_backup"
                print(f"final payload before job creation: {json_payload}")
                status_code,job_id = self.create_workflow(json_payload)
                if status_code == "error":
                    print(f"Error occured while creating job: {job_id}")
                else:
                    print(f"{job_name} job created successfully for customer {customer_name}..!! and job Id is: {job_id}")
            else:
                print(f"Job --> {job_name} is already created..!!")
        except Exception as e:
            print(f"Error occurred while creating job {job_name} for customer {customer_name}..!! {str(e)}")


    #(Currently DEPRECATED). Was used earlier by setup script to create file Trigger job.
    def create_filetrigger_job(self,cloud,job_type,cluster_id):
        try:
            job_name = "Fleming_file_trigger"
            job_status = self.utility.get_jobId_by_name(job_name)
            print(f"job status is: {job_status}")
            if job_status == "not found":
                json_payload = self.get_json_payload_template(cloud,job_type)
                #add job name
                json_payload["name"] = job_name
                #Trigger the job using file arrival method
                json_payload["trigger"]["file_arrival"]["url"] = spark.conf.get('storage_name')
                json_payload["notebook_task"]["base_parameters"]["storage_location"] = spark.conf.get('storage_name')
                #Add the repo name
                repo_path = self.utility.get_remote_repo_path()
                json_payload["notebook_task"]["notebook_path"] = repo_path+"/bronze_fleming_read_csv"
                print(f"final payload before job creation: {json_payload}")
                status_code,job_id = self.create_workflow(json_payload)
                if status_code == "error":
                    print(f"Error occured while creating job: {job_id}")
                else:
                    print(f"{job_name} job created successfully..!! and job Id is: {job_id}")
            else:
                print(f"Job --> {job_name} is already created..!!")
        except Exception as e:
            print(f"Error occurred while creating job for Fleming_read_csv..!! {str(e)}")

    #JSON payload template used by conifer workflow
    def get_json_payload_template(self,customer,job_type):
        if customer == "conifer" and job_type == "conifer_single_task":
            return {"name": "","creator_user_name":"","run_as_user_name":"","run_as_owner":"true","tags": {"name": "File_trigger"},"data_security_mode": "SINGLE_USER","new_cluster": {"spark_version": "12.2.x-scala2.12","node_type_id": "Standard_DS3_v2","num_workers": 3,"runtime_engine": "STANDARD"},"trigger":{"pause_status":"UNPAUSED","file_arrival":{"url":""}},"notebook_task": {"notebook_path": "","base_parameters": {"storage_location":""}},"max_concurrent_runs": 1}
        elif customer == "conifer" and job_type == "conifer_multi_task":
            return  {"name":"","max_concurrent_runs":1,"trigger": {"pause_status": "PAUSED","file_arrival": {"url": ""}},"tasks":[{"task_key":"Fleming_conifer_control","run_if":"ALL_SUCCESS","notebook_task":{"notebook_path":"","base_parameters":{"storage_location":""},"source":"GIT"},"existing_cluster_id":"","timeout_seconds":0,"email_notifications":{},"notification_settings":{"no_alert_for_skipped_runs":"false","no_alert_for_canceled_runs":"false","alert_on_last_attempt":"false"}},{"task_key":"Fleming_conifer_detail","run_if":"ALL_SUCCESS","notebook_task":{"notebook_path":"","base_parameters":{"storage_location":""},"source":"GIT"},"existing_cluster_id":"","timeout_seconds":0,"email_notifications":{},"notification_settings":{"no_alert_for_skipped_runs":"false","no_alert_for_canceled_runs":"false","alert_on_last_attempt":"false"}},{"task_key":"Fleming_conifer_gross","run_if":"ALL_SUCCESS","notebook_task":{"notebook_path":"","base_parameters":{"storage_location":""},"source":"GIT"},"existing_cluster_id":"","timeout_seconds":0,"email_notifications":{},"notification_settings":{"no_alert_for_skipped_runs":"false","no_alert_for_canceled_runs":"false","alert_on_last_attempt":"false"}},{"task_key":"Fleming_conifer_summary","run_if":"ALL_SUCCESS","notebook_task":{"notebook_path":"","base_parameters":{"storage_location":""},"source":"GIT"},"existing_cluster_id":"","timeout_seconds":0,"email_notifications":{},"notification_settings":{"no_alert_for_skipped_runs":"false","no_alert_for_canceled_runs":"false","alert_on_last_attempt":"false"}},{"task_key":"Conifer_excel_to_backup","depends_on":[{"task_key":"Fleming_conifer_control"},{"task_key":"Fleming_conifer_detail"},{"task_key":"Fleming_conifer_gross"},{"task_key":"Fleming_conifer_summary"}],"run_if":"ALL_SUCCESS","notebook_task":{"notebook_path":"","base_parameters":{"storage_location":""},"source":"GIT"},"existing_cluster_id":"","timeout_seconds":0,"email_notifications":{},"notification_settings":{"no_alert_for_skipped_runs":"false","no_alert_for_canceled_runs":"false","alert_on_last_attempt":"false"}},{"task_key": "month_by_month_differential","depends_on": [{"task_key": "Conifer_excel_to_backup"}],"run_if": "ALL_SUCCESS","notebook_task": {"notebook_path": "","base_parameters": {"storage_location": ""},"source": "GIT"},"existing_cluster_id": "","timeout_seconds": 0,"email_notifications": {},"notification_settings": {"no_alert_for_skipped_runs": "false","no_alert_for_canceled_runs": "false","alert_on_last_attempt": "false"}},{"task_key":"Conifer_Silver_Aggregation","depends_on":[{"task_key":"month_by_month_differential"}],"run_if":"ALL_SUCCESS","notebook_task":{"notebook_path":"","base_parameters": {"storage_location": ""},"source":"GIT"},"existing_cluster_id":"","timeout_seconds":0,"email_notifications":{},"notification_settings":{"no_alert_for_skipped_runs":"false","no_alert_for_canceled_runs":"false","alert_on_last_attempt":"false"}},{"task_key":"Conifer_detail_Aggregation","depends_on":[{"task_key":"Conifer_Silver_Aggregation"}],"run_if":"ALL_SUCCESS","notebook_task":{"notebook_path":"","base_parameters": {"storage_location": ""},"source":"GIT"},"existing_cluster_id":"","timeout_seconds":0,"email_notifications":{},"notification_settings":{"no_alert_for_skipped_runs":"false","no_alert_for_canceled_runs":"false","alert_on_last_attempt":"false"}}],"git_source":{"git_url":"","git_provider":"azureDevOpsServices","git_branch":"main"},"tags":{"name":"File_trigger"},"format":"MULTI_TASK"}
        elif customer == "bevcap" and job_type == "bevcap_multi_task":
            return { "name": "", "max_concurrent_runs": 1, "trigger": { "pause_status": "PAUSED", "file_arrival": { "url": "" } }, "tasks": [{"task_key": "bevcap_bronze", "run_if": "ALL_SUCCESS", "notebook_task": { "notebook_path": "", "base_parameters": { "storage_location": "" }, "source": "GIT" }, "existing_cluster_id": "", "timeout_seconds": 0, "email_notifications": {}, "notification_settings": { "no_alert_for_skipped_runs": "false", "no_alert_for_canceled_runs": "false", "alert_on_last_attempt": "false" } },{"task_key": "Bevcap_excel_to_backup","depends_on": [{"task_key": "bevcap_bronze"}],"run_if": "ALL_SUCCESS","notebook_task": {"notebook_path": "","base_parameters": {"storage_location": ""},"source": "GIT"},"existing_cluster_id": "","timeout_seconds": 0,"email_notifications": {},"notification_settings": {"no_alert_for_skipped_runs": "false","no_alert_for_canceled_runs": "false","alert_on_last_attempt": "false"}}, { "task_key": "bevcap_goldReporting", "depends_on": [ { "task_key": "Bevcap_excel_to_backup" } ], "run_if": "ALL_SUCCESS", "notebook_task": { "notebook_path": "", "base_parameters": { "storage_location": "" }, "source": "GIT" }, "existing_cluster_id": "", "timeout_seconds": 0, "email_notifications": {}, "notification_settings": { "no_alert_for_skipped_runs": "false", "no_alert_for_canceled_runs": "false", "alert_on_last_attempt": "false" } } ], "git_source": { "git_url": "", "git_provider": "azureDevOpsServices", "git_branch": "main" }, "tags": { "name": "File_trigger" }, "format": "MULTI_TASK" }

    #function to create conifer workflow by calling REST API. Need to paas json payload.
    def create_workflow(self,jsonpayload):
        try:
            job_id = ""
            dbutils = self.utility.get_db_utils(spark)
            headers = {
            "Authorization": f"Bearer {spark.conf.get('access_token')}",
            "Content-Type": "application/json",
            }
            print(f"header is: {headers}")
            json_payload = json.dumps(jsonpayload)
            job_payload = str(json_payload)
            # Create a new job
            url = self.utility.get_databricks_url()
            response = requests.post(
                f"{url}/api/2.1/jobs/create", headers=headers, data=job_payload
            )
            if response.status_code == 200:
                job_id = response.json()["job_id"]
                return response.status_code,str(job_id)
            else:
                error_response = response.json()
                print(f"Error occurred while creating workflow...!!")
                return "error", error_response["message"]
        except Exception as e:
            return "error occured while creating job " + str(e)