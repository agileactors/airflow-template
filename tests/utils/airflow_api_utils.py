import time
import uuid

import airflow_client
import allure
from airflow_client.client.api import dag_api, dag_run_api, import_error_api, task_instance_api
from airflow_client.client.model.dag import DAG
from airflow_client.client.model.dag_run import DAGRun
from airflow_client.client.model.dag_run_collection import DAGRunCollection

from tests.configuration import config


class Colors:
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    DEFAULT = "\033[0m"


class AirflowApiUtils:
    def __init__(self):
        self.configuration = airflow_client.client.Configuration(
            host=config["AIRFLOW_API_URL"], username=config["AIRFLOW_USERNAME"], password=config["AIRFLOW_PASSWORD"]
        )
        self.api_client = airflow_client.client.ApiClient(self.configuration)
        self.dag_api_instance = dag_api.DAGApi(self.api_client)
        self.dag_run_api_instance = dag_run_api.DAGRunApi(self.api_client)
        self.task_api_instance = task_instance_api.TaskInstanceApi(self.api_client)
        self.dag_import_error_api_instance = import_error_api.ImportErrorApi(self.api_client)

    def run_dag(self, dag_id: str):
        print(f"{Colors.BLUE}Unpause a DAG{Colors.DEFAULT}")
        try:
            dag = DAG(is_paused=False)
            _ = self.dag_api_instance.patch_dag(dag_id, dag, update_mask=["is_paused"])
        except airflow_client.client.exceptions.OpenApiException as e:
            print(f"{Colors.RED}Exception when calling DAGAPI->patch_dag: %s\n{Colors.DEFAULT}" % e)
        else:
            print(f"{Colors.GREEN}Unpausing DAG is successful{Colors.DEFAULT}")

        print(f"{Colors.BLUE}Triggering a DAG run{Colors.DEFAULT}")
        try:
            # Create a DAGRun object (no dag_id should be specified because it is read-only property of DAGRun)
            # dag_run id is generated randomly to allow multiple executions of the script
            dag_run_id = "some_test_run_" + uuid.uuid4().hex
            dag_run = DAGRun(dag_run_id=dag_run_id)
            _ = self.dag_run_api_instance.post_dag_run(dag_id, dag_run)
        except airflow_client.client.exceptions.OpenApiException as e:
            print(f"{Colors.RED}Exception when calling DAGRunAPI->post_dag_run: %s{Colors.RED}" % e)
        else:
            print(f"{Colors.GREEN}Posting DAG Run successful{Colors.DEFAULT}")

        self.wait_dag(dag_id, dag_run_id)

    def wait_dag(self, dag_id: str, dag_run_id: str):
        print(f"{Colors.BLUE}Status of a DAG run{Colors.DEFAULT}")
        try:
            is_not_finished = True
            while is_not_finished:
                api_response = self.dag_run_api_instance.get_dag_run(dag_id, dag_run_id)
                print(api_response.state)
                if str(api_response.state) in ["success", "failed"]:
                    is_not_finished = False
                else:
                    time.sleep(1)

        except airflow_client.client.exceptions.OpenApiException as e:
            print(f"{Colors.RED}Exception when calling DAGRunAPI->get_dag_run: %s\n{Colors.DEFAULT}" % e)
        else:
            print(f"{Colors.GREEN}Getting DAG Run successful{Colors.DEFAULT}")

    def get_dag_info(self, dag_id: str):
        return self.dag_api_instance.get_dag(dag_id)

    def list_dag_ids(self):
        try:
            dags_collection = self.dag_api_instance.get_dags()["dags"]
            dag_ids_collection = [dag.dag_id for dag in dags_collection]
        except airflow_client.client.exceptions.OpenApiException as e:
            print(f"{Colors.RED}\nException when calling DAGAPI->get_dags: %s{Colors.RED}" % e)
            raise e

        return dag_ids_collection

    def list_dag_runs(self, dag_id: str) -> DAGRunCollection:
        try:
            dag_run_collection = self.dag_run_api_instance.get_dag_runs(dag_id)
        except airflow_client.client.exceptions.OpenApiException as e:
            print(f"{Colors.RED}\nException when calling DAGRunAPI->get_dag_runs: %s\n{Colors.RED}" % e)
            raise e
        return dag_run_collection

    def list_dag_run_ids(self, dag_id: str):
        return [dag_run.dag_run_id for dag_run in self.list_dag_runs(dag_id).dag_runs]

    def get_dag_run_logs(self, dag_id: str, task_id: str):
        dag_run_ids = self.list_dag_run_ids(dag_id)
        dag_run_logs = []

        for index, dag_run_id in enumerate(dag_run_ids):
            log = self.task_api_instance.get_log(dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, task_try_number=index + 1)
            dag_run_logs.append(log.content)

        return dag_run_logs

    def delete_dag_run(self, dag_id: str, dag_run_id: str):
        try:
            _ = self.dag_run_api_instance.delete_dag_run(dag_id, dag_run_id)
        except airflow_client.client.exceptions.OpenApiException as e:
            print(f"{Colors.RED}Exception when calling DAGRunAPI->delete_dag_run: %s\n{Colors.RED}" % e)
            raise e

    def get_import_errors(self):
        return self.dag_import_error_api_instance.get_import_errors()

    def delete_dag(self, dag_id):
        self.dag_api_instance.delete_dag(dag_id)

    def delete_all_dag_runs(self):
        dag_ids = self.list_dag_ids()

        for dag_id in dag_ids:
            dag_runs = self.list_dag_run_ids(dag_id)
            if dag_runs:
                for dag_run in dag_runs:
                    self.delete_dag_run(dag_id, dag_run)
