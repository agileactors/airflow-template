"""
Module containing helper functions for dag-factory.
"""

import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Pattern, Union

import pendulum
from airflow.utils.module_loading import import_string
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException


def merge_configs(config: Dict[str, Any], default_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merges a default config with DAG config. Used to set default values
    for a group of DAGs.

    Args:
        config: config to merge in default values
        default_config: config to merge default values from

    Returns:
        Dictionary containing merged config.
    """

    for key in default_config:
        if key in config:
            if isinstance(config[key], dict) and isinstance(default_config[key], dict):
                merge_configs(config[key], default_config[key])
        else:
            config[key] = default_config[key]
    return config


def check_dict_key(item_dict: Dict[str, Any], key: str) -> bool:
    """
    Check if key is in dictionary and is not None.

    Args:
        item_dict: dictionary to check
        key: key to check

    Returns:
        True if key is valid, False otherwise.
    """
    return key in item_dict and item_dict[key] is not None


def set_callable(params: Dict[str, Any], callback_name: str) -> None:
    """
    Replace a string reference to a callable with the callable. Must be
    able to import the callable.

    If callback name is not in params, nothing happens.

    Args:
        params: dictionary containing parameters
        callback_name: name of callback to replace

    Raises:
        DagFactoryConfigException: if params[callback_name] is not a string
        DagFactoryException: if callback cannot be imported
    """
    if check_dict_key(params, callback_name):
        if isinstance(params[callback_name], str):
            try:
                params[callback_name] = import_string(params[callback_name])
            except Exception as err:
                logging.error("Failed to import %s : %s", params[callback_name], str(err))
                raise DagFactoryException("Failed to import callback") from err
        elif callable(params[callback_name]):
            logging.info("Callback already set. Ignoring.")
        else:
            logging.error("%s must be a string or callable", params[callback_name])
            raise DagFactoryConfigException("Invalid callback name.")


def check_template_searchpath(template_searchpath: Union[str, List[str]]) -> bool:
    """
    Checks if jinja template_searchpath is valid.

    Args:
        template_searchpath: path or list of paths to search for jinja templates

    Returns:
        True if template_searchpath is valid, False otherwise.
    """
    if isinstance(template_searchpath, str):
        if not os.path.isabs(template_searchpath):
            logging.error("Template searchpath isn't absolute. Ignoring it.")
            return False
        if not os.path.isdir(template_searchpath):
            logging.error("Template searchpath doesn't exist. Ignoring it.")
            return False
        return True
    if isinstance(template_searchpath, list):
        for path in template_searchpath:
            if not os.path.isabs(path):
                logging.error(
                    "Template searchpath isn't absolute. Ignoring it.",
                )
                return False
            if not os.path.isdir(path):
                logging.error(
                    "Template searchpath doesn't exist. Ignoring it.",
                )
                return False
        return True
    return False
