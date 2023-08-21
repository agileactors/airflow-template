# Airflow Template


1. [Setup](#setup)
2. [Testing](#testing)
    1. [Unit Testing](#unit-testing)
    2. [Integration Testing](#integration-testing)
    3. [Manual Testing](#manual-testing)
3. [CI/CD](#cicd)
    1. [CI](#ci)[requirements-dev.txt](requirements-dev.txt)
    2. [CD](#cd)
4. [Code Structure](#code-structure)


## Setup

- It is recommended to use [pyenv](https://github.com/pyenv/pyenv), a CLI tool that allows multiple versions of Python to be
  installed separately. Follow the [installation instructions](https://github.com/pyenv/pyenv#installation)
  for your platform and run:

  ```
  pyenv install
  ```

- This will download and install Python **3.10.11** which is specified in the `.project-version` file which in turn is created by the command `pyenv local 3.10.11`. This use of pyenv ensures the pinning and usage of the specified Python version.

  > Note: pyenv downloads and compiles the version of Python you install, which means you may need
  > to also install some libraries if not present in your system, please follow the
  > [common build problems wiki](https://github.com/pyenv/pyenv/wiki/Common-build-problems) for
  > your platform.
  > 
  > If you already have Python 3.10.11 installed you do not need to reinstall it and pyenv should automatically use the correct version due to the pinning file `.project-version`

- Create a virtualenv:

  ```bash
    python -m venv .venv
    source .venv/bin/activate
  ```
  This virtualenv now has the version of Python which was set by pyenv and the .project-version file.
  
  > Note: the rest of these instructions assume you've activated the virtualenv as does the Makefile. You may want to use a virtualenv tool like
  > [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/) or
  > [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv).
  
- Install dependencies via:
  ```bash
    make install-dev
  ```
- Please do not use pip by hand as the makefile contains the explicit activation of pre-commit hooks which will be necessary.

- Then run docker-compose up to start the deployment so that you can run the tests .
 ```bash
    make build-image
  ```

### Setup operational event database

Change the environment variable OPERATIONAL_DB_URL to point to the proper PostgreSQL database. By default points to the one
used for integration testing, in [docker-compose-test.yaml](docker-compose-test.yaml) as 

```text
postgresql://postgres:example@localhost:5432/postgres
```

If there are special character in password/username like the ones provided by AWS, you need to url encode them
through some service , e.g. [here](https://www.urlencoder.org/).


## Testing

### Unit Testing
Tested with python 3.10.11.

```bash
  pip install -r requirements.txt
```

- Run all tests with:
```bash
  make unit
 ```

### Integrity Tests

This is a special category of tests related to dag integrity. To run the integrity suite run:

```bash
  make integrity
```

This will inform about import errors. If you need to test a dag for import errors you can run:

```bash
  CONFIG="your_favourite_config.yaml,your_second_favourite_config.yaml" make check-for-import-error 
```

(the CONFIG variable is a comma separated list of config files, or a single config file)

### Integration Testing

Part of the automated tests. If you need to test manually, it is as easy as executing
```bash
  make integration-tests
```

or specifically

```bash
  make integration-environment
  pytest -v -s tests/integration --no-header -vv || (make teardown && exit 1)
  make integration-teardown
```

The integration tests need a running environment consisting of:

- A running airflow docker container
- An operational events postgres database    
- A S3 bucket emulated by [a docker image of localstack](https://docs.localstack.cloud/references/docker-images/)

The test dag is triggered, assertions run and setup is cleaned up. These test roles are already present in [docker-compose-test.yaml](docker-compose-test.yaml)
More information can be found here: https://newcross.atlassian.net/wiki/spaces/DET/pages/3871113257/Integration+Testing


To run both unit and integration together run:

```bash
  make test
```


### Manual Testing

Here we run manually the steps leading to the integration tests. This can be useful for debugging purposes and local development.
To create a local environment with prepopulated test data you can run:

```bash
  make integration-environment
```

You can find 

- the airflow ui on localhost:8080, username and password are both `airflow`.
- the postgres database on localhost:5432, username is `postgres` and password `example`

(if you need things to be stateful, uncomment postgres and mssql volumes on the [docker-compose-test.yaml](docker-compose-test.yaml) file)

### Run the dag

Visit the [airflow server](http://localhost:8080) and activate your dag called `testing_integration_dag`. 
Extra info in official Airflow Documentation [Airflow Apache Project](https://airflow.apache.org/).

## CI/CD

The CI/CD pipeline is configured in the [GitHub actions](.github/workflows) files. 

### CI
The CI pipeline is configured in the [GitHub actions](.github/workflows/ci.yml) file. 
- It is triggered on every push to the main branch and in every pull request(pr).
- It runs the versioning, tagging the code, unit testing and the integration testing.
- It also runs the pre-commit hooks to ensure that the code is formatted correctly and that the tests pass before pushing to the main branch.
- It also runs the SonarCloud analysis to check the code quality and the code coverage.
  - The SonarCloud analysis is configured in the [sonar-project.properties](sonar-project.properties) file.
  - You can see the SonarCloud quality gate for NewCross in the https://sonarcloud.io/organizations/newcross-tech/quality_gates/show/9


Part of the CI pipeline is the mutation testing workflow which is configured in the [GitHub actions](.github/workflows/mutation-testing.yml) file.
- It is triggered on every push to the main branch and in every pull request(pr).
- It runs the mutation testing and generates a report.
- Fails if the survival rate is more than 80%.

### CD
The CD pipeline is configured in the [GitHub actions](.github/workflows/deploy-to-env.yml) file.
- It is triggered manually on every push to the main branch and runs only with tags.
- You need to choose the tag you want to deploy and the environment you want to deploy to.
- Deploys the code to the environment that you have chose with rsync to the dag file.
  - For now, we don't have a PRODUCTION environment so the only option is to deploy to the DEVELOPMENT environment.

More information can be found here: https://newcross.atlassian.net/wiki/spaces/DET/pages/3857285133/Data+Ingestion+CI+CD+Github+Pipeline

## Code Structure
```
.
├── .github                     # Directory for GitHub actions
│   └── workflows               # Directory for GitHub actions workflows
├── ci_scripts                  # Directory where any scripts that your CI/CD might need to access
├── dags                        # Directory where all your DAGs go
│   ├── dagfactory              # Directory for dynamically generating DAGs
│   ├── models                  # Directory for ORM models
│   ├── operators               # Directory for custom Airflow operators
│   └── discover_dags.py        # Directory for dynamically generating DAGs from yaml files
├── plugins                     # Directory for any custom or community Airflow plugins
│   └── example-plugin.py              
├── tests                       # Directory for all tests
│   ├── unit                    # Directory for unit tests
│   ├── resources               # Directory for testing resources
│   └── integration             # Directory for integration tests
├── .gitignore                  # File for ignoring files in git
├── .pre-commit-config.yaml     # File for running pre-commit hooks before puss to git
├── docker-compose-test.yaml    # File for set up Testing environment 
├── Dockerfile-test             # File for AirFlow/python Docker image for testing
├── Makefile                    # File for any Python packages 
├── mutation_config.toml        # File of mutation testing configurations
├── pyproject.toml              # File of python project configurations
├── requirements.txt            # File for any Python packages prod 
├── pytest.ini                  # File to add test paths
└── setup.cfg                   # File for set up

```