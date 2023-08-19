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


def get_datetime(date_value: Union[str, datetime, date], timezone: str = "UTC") -> datetime:
    """
    Converts a string, datetime or date to a datetime object.

    Defaults to today, if not a valid date or relative time.

    Args:
        date_value: string, datetime or date to convert
        timezone: string representing timezone for the DAG

    Returns:
        datetime object
    """
    try:
        local_tz = pendulum.tz.timezone(timezone)
    except Exception as err:
        logging.error("Failed to create timezone given value: %s", timezone)
        raise DagFactoryException("Failed to create timezone") from err

    if isinstance(date_value, datetime):
        return date_value.replace(tzinfo=local_tz)
    elif isinstance(date_value, date):
        return datetime.combine(date_value, datetime.min.time()).replace(tzinfo=local_tz)

    # Try parsing as date string
    try:
        return pendulum.parse(date_value).replace(tzinfo=local_tz)  # type: ignore
    except pendulum.exceptions.ParserError:
        # Parse as relative time string
        rel_delta: timedelta = get_time_delta(date_value)
        now: datetime = datetime.today().replace(tzinfo=local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        if not rel_delta:
            return now
        else:
            return now - rel_delta


def get_time_delta(time_string: str) -> timedelta:
    """
    Take a time string and returns timedelta object.
    Format of string should be a number followed by a unit of time
    (e.g. 2 hours, 10 days, 4 minutes, 10 seconds).

    Args:
        time_string: string representing relative time

    Returns:
        timedelta object
    """
    rel_time: Pattern = re.compile(
        pattern=r"((?P<hours>\d+?)\s+hour)?((?P<minutes>\d+?)\s+minute)?((?P<seconds>\d+?)\s+second)?((?P<days>\d+?)\s+day)?",  # noqa
        flags=re.IGNORECASE,
    )
    parts_ = rel_time.match(string=time_string)
    if not parts_:
        logging.error("Invalid relative time: %s", time_string)
        raise DagFactoryException("Invalid relative time")

    # https://docs.python.org/3/library/re.html#re.Match.groupdict
    parts: Dict[str, str] = parts_.groupdict()
    time_params = {}
    if all(value is None for value in parts.values()):
        logging.error("Invalid relative time: %s", time_string)
        raise DagFactoryException("Invalid relative time")
    for time_unit, magnitude in parts.items():
        if magnitude:
            time_params[time_unit] = int(magnitude)
    return timedelta(**time_params)


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
