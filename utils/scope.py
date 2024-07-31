import requests,json
from utils import utilities
from pyspark.sql import SparkSession

#get existing spark object using sparkSession
spark = SparkSession.builder.appName('fleming').getOrCreate()
utility = utilities.utility()
dbutils = utility.get_dbutils(spark)

#Generic class to create databricks Scopes and secrets using REST API
class secretScope:
    def __init__(self):
        self.databricks_host = utility.get_databricks_url()
        print(self.databricks_host)

    #Create a scope using provided name and create a secret(<scope_name>_secret) and assign prvided value to it
    def create_secret_scope(self,scope_name,access_token,secret_value):
        #creating a secret scope
        utility = utilities.utility()
        data = str(json.dumps({"scope":scope_name}))
        databricks_host = self.databricks_host
        print(databricks_host)
        url = '{}/api/2.0/secrets/scopes/create'.format(databricks_host)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        scope_exist = self.check_secret_scope(scope_name,headers,databricks_host)
        if not scope_exist:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                print(f"Scope {scope_name} created Successfully..!!")
                token_res = self.create_scope_secret(headers,scope_name,secret_value,databricks_host)
                if token_res:
                    return True
            else:
                return False
        else:
            return "Scope already exist..!! "+str(scope_name)

    #create a secret under a scope and assign value to it
    def create_scope_secret(self,headers,scope_name,secret_value,databricks_host):
        data = str(json.dumps({"scope": scope_name,"key":scope_name+"_secret","string_value":secret_value}))
        url = '{}/api/2.0/secrets/put'.format(databricks_host)
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            print(f"secret for scope {scope_name} created successfully")
            return True
        else:
            return False
    
    def update_secret_scope(self,scope_name,access_token,secret_value):
        utility = utilities.utility()
        data = str(json.dumps({"scope":scope_name, "key": scope_name+"_secret"}))
        databricks_host = self.databricks_host
        print(databricks_host)
        url = '{}/api/2.0/secrets/delete'.format(databricks_host)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        scope_exist = self.check_secret_scope(scope_name,headers,databricks_host)
        print(f"scope exist status: {scope_exist}")
        if scope_exist:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                print(f"Previous scope {scope_name} deleted Successfully..!!")
            response_new = self.create_scope_secret(headers,scope_name,secret_value,databricks_host)
            if response_new:
                print(f"secret for scope {scope_name} Updated successfully")
                return True
            else:
                return False
        else:
            print(f"Scope does not exist..!!")


    #check whether the scope already exist or not
    def check_secret_scope(self,scope_name,headers,databricks_host):
        #check whether scope exist or not
        url = '{}/api/2.0/secrets/scopes/list'.format(databricks_host)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            dct = response.json()
            if dct:
                print(f"checking scope exist or not : {scope_name}")
                for scp_name in dct["scopes"]:
                    if scope_name in scp_name["name"]:
                        return True
                else:
                    return False
            else:
                return False
        else:
            return False