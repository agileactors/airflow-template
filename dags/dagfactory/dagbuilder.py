"""
Module containing code for generating tasks and constructing a DAG.
"""

import logging
import os
from datetime import timedelta
from typing import Any, Callable, Dict, List, Union

from airflow import configuration
from airflow.models import DAG, BaseOperator
from airflow.utils.module_loading import import_string
from airflow.utils.task_group import TaskGroup
from dagfactory import utils
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException

SYSTEM_PARAMS: List[str] = ["operator", "dependencies", "task_group_name"]


class DagBuilder:
    """
    Generates tasks and a DAG from a config.

    Args:
        dag_name: name of the DAG
        dag_config: dictionary containing config for the DAG
        default_config: dictionary containing default config for the DAG
    """

    def __init__(self, dag_name, dag_config: Dict[str, Any], default_config: Dict[str, Any]) -> None:
        self.dag_name = dag_name
        self.dag_config = dag_config
        self.default_config = default_config

    def get_dag_params(self) -> Dict[str, Any]:
        """
        Creates DAG config dictionary with parameters that will be passed
        to DAG constructor. Ensures that DAG config contains all required
        parameters in the correct type. Merges default with non-default
        parameters.

        Returns:
            Dictionary containing DAG parameters.
        """
        try:
            dag_params: Dict[str, Any] = utils.merge_configs(self.dag_config, self.default_config)
        except Exception as err:
            logging.error("Failed to merge config with default config %s", str(err))
            raise DagFactoryConfigException("Failed to merge config with default config") from err

        dag_params["dag_id"] = self.dag_name

        # Convert from 'end_date: Union[str, datetime, date]' to
        if utils.check_dict_key(dag_params["default_args"], "end_date"):
            dag_params["default_args"]["end_date"] = utils.get_datetime(
                date_value=dag_params["default_args"]["end_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )

        # Convert from 'dagrun_timeout_sec: int' to 'dagrun_timeout: timedelta'
        if utils.check_dict_key(dag_params, "dagrun_timeout_sec"):
            dag_params["dagrun_timeout"] = timedelta(seconds=dag_params["dagrun_timeout_sec"])
            del dag_params["dagrun_timeout_sec"]

        # Set 'retry_delay' from 'retry_delay_sec'
        if utils.check_dict_key(dag_params["default_args"], "retry_delay_sec"):
            dag_params["default_args"]["retry_delay"] = timedelta(seconds=dag_params["default_args"]["retry_delay_sec"])
            del dag_params["default_args"]["retry_delay_sec"]

        # Set 'sla' from 'sla_secs'
        if utils.check_dict_key(dag_params["default_args"], "sla_secs"):
            dag_params["default_args"]["sla"] = timedelta(seconds=dag_params["default_args"]["sla_secs"])
            del dag_params["default_args"]["sla_secs"]

        # Set callable functions in case of success, failure, etc.
        utils.set_callable(dag_params["default_args"], "on_success_callback")
        utils.set_callable(dag_params["default_args"], "on_failure_callback")
        utils.set_callable(dag_params["default_args"], "sla_miss_callback")
        utils.set_callable(dag_params["default_args"], "on_retry_callback")
        utils.set_callable(dag_params, "on_success_callback")
        utils.set_callable(dag_params, "on_failure_callback")
        utils.set_callable(dag_params, "sla_miss_callback")
        # on_retry_callback can only be in default args. Otherwise, it
        # should be set in the task level.

        # validate template_searchpath
        if utils.check_dict_key(dag_params, "template_searchpath"):
            if not utils.check_template_searchpath(dag_params["template_searchpath"]):
                logging.error("Invalid template search path")
                raise DagFactoryConfigException("Invalid template search path")

        # Ensure 'start_date' is in default_args and in correct format
        # TODO maybe not mandatory
        try:
            dag_params["default_args"]["start_date"] = utils.get_datetime(
                date_value=dag_params["default_args"]["start_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )
        except KeyError as err:
            logging.error("%s DAG config missing start_date", self.dag_name)
            raise DagFactoryConfigException("DAG config missing start_date") from err

        return dag_params

    @staticmethod
    def make_task(operator: str, task_params: Dict[str, Any]) -> BaseOperator:
        """
        Takes an operator and params and creates an instance of that operator.

        Args:
            operator: name of the operator. Must be a dotted module path.
            :param task_params:
        """
        # If python operator is used, set callable function (required)
        if operator == "airflow.operators.python.PythonOperator":
            if not utils.check_dict_key(task_params, "python_callable"):
                logging.error("PythonOperator requires python_callable")
                raise DagFactoryConfigException("PythonOperator requires python_callable")

            utils.set_callable(task_params, "python_callable")

        # Set callable functions for http operators
        if (
            operator == "airflow.providers.http.operators.http.SimpleHttpOperator"
            or operator == "airflow.providers.http.sensors.http.HttpSensor"
        ):
            if utils.check_dict_key(task_params, "response_filter"):
                logging.info("Setting response_filter to %s", task_params["response_filter"])
                utils.set_callable(task_params, "response_filter")

            if utils.check_dict_key(task_params, "response_check"):
                logging.info("Setting response_check to %s", task_params["response_check"])
                utils.set_callable(task_params, "response_check")

        try:
            operator_obj: Callable[..., BaseOperator] = import_string(operator)
        except Exception as err:
            logging.error("Failed to import operator %s", operator)
            raise DagFactoryException("Failed to import operator") from err

        try:
            task: BaseOperator = operator_obj(**task_params)
        except Exception as err:
            logging.error(str(err))
            logging.error("Failed to create operator %s", operator)
            raise DagFactoryException("Failed to create operator") from err

        return task

    # TODO remove default empty dicts when task groups are added
    @staticmethod
    def set_dependencies(  # pylint: disable=dangerous-default-value
        tasks_config: Dict[str, Dict[str, Any]],
        operators_dict: Dict[str, BaseOperator],
        task_groups_config: Dict[str, Dict[str, Any]] = {},
        task_groups_dict: Dict[str, TaskGroup] = {},
    ) -> None:
        """
        Take the task configurations in YAML file and operator
        instances, then set the dependencies between tasks.

        Args:
            tasks_config: Raw task config from YAML file
            operators_dict: Dictionary for operator instances
            :param operators_dict:   Dictionary for operator instances
            :param tasks_config:     Raw task config from YAML file
            :param task_groups_dict:    Dictionary for task group instances
            :param task_groups_config:  Raw task group config from YAML file
        """
        tasks_and_task_groups_config: Dict[str, Dict[str, Any]] = {**tasks_config, **task_groups_config}
        tasks_and_task_groups_instances: Dict[str, Union[BaseOperator, TaskGroup]] = {**operators_dict, **task_groups_dict}
        for name, conf in tasks_and_task_groups_config.items():
            # if task is in a task group, group_id is prepended to its name
            if conf.get("task_group"):
                group_id = conf["task_group"].group_id
                name = f"{group_id}.{name}"
            if conf.get("dependencies"):
                source: Union[BaseOperator, "TaskGroup"] = tasks_and_task_groups_instances[name]
                for dep in conf["dependencies"]:
                    if tasks_and_task_groups_config[dep].get("task_group"):
                        group_id = tasks_and_task_groups_config[dep]["task_group"].group_id
                        dep = f"{group_id}.{dep}"
                    dep = tasks_and_task_groups_instances[dep]
                    source.set_upstream(dep)

    @staticmethod
    def make_task_groups(task_groups: Dict[str, Any], dag: DAG) -> Dict[str, TaskGroup]:  # pylint: disable=unused-argument
        """
        TODO
        """
        task_groups_dict: Dict[str, TaskGroup] = {}

        return task_groups_dict

    def build(self) -> Dict[str, Union[str, DAG]]:
        """
        Generates a DAG from DAG parameters.

        Returns:
            Dictionary containing DAG name and DAG object.
        """

        # This is parsed from YAML config file
        dag_params: Dict[str, Any] = self.get_dag_params()

        # ____SET DAG PARAMETERS BASED ON CONFIG AND DEFAULT VALUES____

        # This will be passed to DAG constructor
        dag_kwargs: Dict[str, Any] = {}

        dag_kwargs["dag_id"] = dag_params["dag_id"]

        # If schedule interval not specified defaults to 1 day
        dag_kwargs["schedule"] = dag_params.get("schedule", timedelta(days=1))

        dag_kwargs["description"] = dag_params.get("description", None)

        dag_kwargs["max_active_tasks"] = dag_params.get(
            "max_active_tasks", configuration.conf.getint("core", "max_active_tasks_per_dag")
        )

        dag_kwargs["catchup"] = dag_params.get("catchup", configuration.conf.getboolean("scheduler", "catchup_by_default"))

        dag_kwargs["max_active_runs"] = dag_params.get(
            "max_active_runs",
            configuration.conf.getint("core", "max_active_runs_per_dag"),
        )

        dag_kwargs["dagrun_timeout"] = dag_params.get("dagrun_timeout", None)

        dag_kwargs["default_view"] = dag_params.get("default_view", configuration.conf.get("webserver", "dag_default_view"))

        dag_kwargs["orientation"] = dag_params.get("orientation", configuration.conf.get("webserver", "dag_orientation"))

        dag_kwargs["template_searchpath"] = dag_params.get("template_searchpath", None)

        dag_kwargs["render_template_as_native_obj"] = dag_params.get("render_template_as_native_obj", False)

        dag_kwargs["sla_miss_callback"] = dag_params.get("sla_miss_callback", None)

        dag_kwargs["on_success_callback"] = dag_params.get("on_success_callback", None)

        dag_kwargs["on_failure_callback"] = dag_params.get("on_failure_callback", None)

        dag_kwargs["default_args"] = dag_params.get("default_args", None)

        dag_kwargs["doc_md"] = dag_params.get("doc_md", None)

        # If doc_md not found, try to set it from specified file
        if not (dag_kwargs["doc_md"]) and (dag_params.get("doc_md_file_path", None)):
            if not os.path.isabs(dag_params.get("doc_md_file_path")):  # type: ignore
                logging.error(" Doc_md file path must be absolute path")
                raise DagFactoryConfigException(" Doc_md file path must be absolute path")

            try:
                with open(dag_params["doc_md_file_path"], "r", encoding="utf-8") as file:
                    dag_kwargs["doc_md"] = file.read()
            except Exception as err:
                logging.error("Failed to read doc_md_file_path: %s", str(err))
                raise DagFactoryConfigException("Failed to read doc_md_file_path") from err

        dag_kwargs["access_control"] = dag_params.get("access_control", None)

        dag_kwargs["is_paused_upon_creation"] = dag_params.get("is_paused_upon_creation", None)

        dag_kwargs["tags"] = dag_params.get("tags", None)

        dag_kwargs["params"] = dag_params.get("params", None)

        # Finally create DAG object
        try:
            dag: DAG = DAG(**dag_kwargs)
        except Exception as err:
            logging.error("Failed to create DAG %s: %s", dag_kwargs["dag_id"], str(err))
            raise DagFactoryException("Failed to create DAG") from err

        # mark the object as auto generated
        dag.is_dagfactory_auto_generated = True  # type: ignore[attr-defined]

        tasks: Dict[str, Dict[str, Any]] = dag_params["tasks"]

        # create dictionary of task groups
        task_groups_dict: Dict[str, "TaskGroup"] = self.make_task_groups(dag_params.get("task_groups", {}), dag)

        # create dictionary to track tasks and set dependencies
        tasks_dict: Dict[str, BaseOperator] = {}

        for task_name, task_conf in tasks.items():
            task_conf["task_id"] = task_name
            operator: str = task_conf["operator"]
            task_conf["dag"] = dag
            # add task to task_group
            if task_groups_dict and task_conf.get("task_group_name"):
                task_conf["task_group"] = task_groups_dict[task_conf.get("task_group_name")]  # type: ignore[index]

            params: Dict[str, Any] = {k: v for k, v in task_conf.items() if k not in SYSTEM_PARAMS}
            task: BaseOperator = DagBuilder.make_task(operator=operator, task_params=params)
            tasks_dict[task.task_id] = task

        # Set task dependencies
        self.set_dependencies(tasks, tasks_dict, dag_params.get("task_groups", {}), task_groups_dict)

        return {"dag_id": dag_params["dag_id"], "dag": dag}
