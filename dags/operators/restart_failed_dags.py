import logging

from airflow.api.common import trigger_dag
from airflow.models import DagRun
from operators.exceptions import OperatorException


def restart_failed_dags(**context):
    """
    Restart all failed DAGs.
    :param context: Airflow context
    """
    try:
        # Get all the failed DAG runs
        failed_dag_runs = DagRun.find(state="failed", execution_start_date=context["data_interval_start"])
        logging.info(f"Failed DAG runs: {failed_dag_runs}")

        # Restart each failed DAG run
        for dag_run in failed_dag_runs:
            try:
                trigger_dag.trigger_dag(dag_run.dag_id, run_id=f"re-run_{dag_run.run_id}")  # Trigger the DAG
                logging.info(f"Successfully restarted dag_run: {dag_run}")
            except Exception as e:
                logging.info(f"Failed to restart dag_run: {dag_run}. Error: {str(e)}")
    except Exception as e:
        logging.info(f"Failed to find DAGs. Error: {str(e)}")
        raise OperatorException(f"Failed to find DAGs. Error: {str(e)}")
