## Why Airflow?
- Airflow is a platform to programmatically author, schedule and monitor workflows.
- Use airflow to author workflows as directed acyclic graphs (DAGs) of tasks.
- The airflow scheduler executes your tasks on an array of workers while following the specified dependencies.
- Rich command line utilities make performing complex surgeries on DAGs a snap.
- The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.
- Simpler way to write complex pipelines with ASYNC tasks than using microservices(FASTAPI or other python frameworks) or cron or shell scripts.

## How we structure Airflow code?
Before we start we need at first to explain how we structure a python project :

### Anatomy of a Python Project:
1. #### Project Root:
    This is the main folder of your project.Give it a meaningful name related to your project.
2. #### README.md:
    A file explaining what your project does, how to set it up, and any other important information.
3. #### src (Source Code):
    The src folder holds your actual Python code.
    Organize code into packages or modules based on functionality.
4. #### Tests:
    Create a separate tests folder to store your test files.
    Use a testing framework like unittest or pytest.
5. #### Requirements:
    Include a requirements.txt file listing project dependencies.
    Helps others to install necessary packages with a single command: pip install -r requirements.txt.
6. #### Virtual Environment:
    Encapsulate project dependencies using a virtual environment.
    Create it using python -m venv venv and activate it.
7. #### Configurations:
    A config folder for configuration files.
    Store settings, constants, or environment variables here.
8. #### Documentation:
    A docs folder for documentation files.
    Use tools like Sphinx to generate documentation from docstrings.
9. #### Utilities:
    A utils folder for utility scripts or functions.
    Keep reusable code separate from main application logic.
10. #### .gitignore:
    Specify files or directories to be ignored by version control (e.g., __pycache__, virtual environment).
11. #### CI/CD Configuration:
    If using continuous integration or continuous deployment, include configuration files (e.g., .travis.yml, .github/workflows).
12. #### Logging:
    Implement a logging strategy, potentially with a logs folder.

### Additional Tips:
- Consistency is Key:

- Follow naming conventions (e.g., PEP 8).
- Maintain a consistent folder structure.
- Keep It Modular:
Break down your code into manageable modules or packages.
- Documentation Matters:
- Write clear and concise comments,docstrings and type hints.
- Automate What You Can:
Use tools like black for code formatting, isort for import sorting, and linters for code quality.

#### What tools we should use and How we set up this tools?
Those tools are used to automate the process of code formatting, import sorting, and code quality checking.
To keep the code consistent and maintainable for every member of the team.
##### Tools:
-  Black: 
Black is a Python code formatter for automatic code formatting.
- Flake8:
Flake8 is a Python linter for checking code quality.
- Isort:
Isort is a Python utility for sorting imports alphabetically.
- mypy:
Mypy is a static type checker for Python.

##### Setup:
- Create a pyproject.toml file in the project root.Pyproject.toml defines project configuration, dependencies, and build settings in Python projects.
and a setup.cfg file in the project root. Setup.cfg defines project metadata and configuration options for tools like flake8.
We have both files because Black uses pyproject.toml and Flake8 uses setup.cfg
There we will set up the tools we want to use.
- Install pre-commit via pip install pre-commit.
- Create a .pre-commit-config.yaml file in the project root.This file defines the pre-commit hooks to be used.
##### What is pre-commit?
"pre-commit" is a tool that manages and runs pre-commit hooks. These hooks are scripts or commands that run before each commit.
There we will set up the tools we want to use to check our code before pushing it to the repository.

For the simplicity of running all that we have set up I used to create a Makefile
(A Makefile is a configuration file used in software development projects to define a set of tasks and their dependencies) 
to create the commands to help me run this tools faster.

### Airflow Project Structure:
Our Airflow project enhances uptime by storing DAGs centrally, using a generic storage system. 
The scheduler regularly syncs, allowing seamless updates every 30 seconds, ensuring flexibility without service interruptions.
This project has only 2 functionalities one to read data from a pg database and the other to restart every failed dags.
- #### .github:
  - #### workflows:
    This folder contains the github actions workflows.
  - #### pull_request_template.md:
    This file contains the template for the pull request.
- #### ci_scripts:
    This folder contains any scripts that our CI/CD might need to access.
- #### dags:
    This folder contains all our source code
- #### plugins:
    This folder contains any custom or community Airflow plugins.
- #### tests:
    This folder contains all our tests.

### Why Testing?
- Testing is a critical part of software development.
- It helps to ensure that our code is working as expected.
- It helps to prevent bugs and errors.
- It helps to ensure that our code is maintainable.
- It helps to ensure that our code is readable.

#### How we test our code?
- Unit Testing:
    - Unit testing is a type of testing that focuses on testing individual units of code.
    - A unit is the smallest testable part of any software.
    - In Python, a unit is usually a function, method, or class.
    - Unit tests are usually written by the developers themselves.
    - Unit tests are usually written before the code itself.
    - Unit tests are usually written using a testing framework like unittest or pytest.
    - Unit tests are usually run locally.
    - Unit tests are usually run automatically as part of the CI/CD pipeline.
- Integration Testing:
    - Integration testing is a type of testing that focuses on testing the integration between different units of code.
    - Integration tests are usually written by the developers themselves.
    - Integration tests are usually written after the code itself.
    - Integration tests are usually written using a testing framework like unittest or pytest.
    - Integration tests are usually run locally.
    - Integration tests are usually run automatically as part of the CI/CD pipeline.
#### How we test our Airflow code?
- Integration Testing:
  - We use the docker-compose-test.yaml file to set up the testing environment.
    Which contains the following services:
    - Airflow 
      - Postgres: Handles persistent storage of Airflow metadata, including DAG definitions, task instances, and execution logs.
      - Redis: Serves as a message broker for Celery, enabling parallel task execution across multiple workers.
      - Flower: Provides a web interface for monitoring and administrating Celery clusters.
      - Airflow Scheduler: Orchestrates the scheduling of DAG runs and triggers the execution of individual tasks within DAGs based on their defined schedules.
      - Airflow Webserver: Provides a web-based interface for users to interact with and monitor Airflow, view DAGs, and manage workflows.
      - Airflow Worker: Executes tasks on behalf of the scheduler.
      - Airflow Init: Checks Airflow version compatibility, warns about resource constraints, and sets up the necessary directory structure.
      - Airflow CLI: Provides a command-line interface for interacting with Airflow.
    - External Postgres
  - We run the command:
  ```bash
    make integration-environment
  ```


