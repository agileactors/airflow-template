"""
Discover Airflow DAGs from YAML config files.
"""

from dagfactory.dagfactory import load_yaml_dags

# YAML files should be in the dags folder
load_yaml_dags(globals_dict=globals())
