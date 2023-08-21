.PHONY: install-dev start-db stop-db devserver prodserver clean check format tests

## ATTENTION! activate virtual environment before running!

##  install packages, install pre-commit
install-dev:
	pip3 install -U pip wheel setuptools
	pip3 install -r requirements.txt
	pre-commit install

## clear all caches
clear:
	rm -rf logs
	rm -rf localstack
	rm -rf .mypy_cache
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf mutation.sqlite
	rm -rf report.html
	rm -rf coverage-reports
	rm -rf htmlcov
	rm -rf tests/resources/tmp_configs

## uninstall all dev packages
uninstall-dev:
	pip freeze | xargs pip uninstall -y

## Run linting checks
check:
	isort --check dags tests 	# setup.cfg
	black --check dags tests 		# pyproject.toml
	flake8 dags tests	 # setup.cfg
	mypy dags tests	 --explicit-package-bases --python-version=3.10	# setup.cfg

## reformat the files using the formatters
format:
	isort dags tests
	black dags tests

## down build docker image
drop-image:
	docker compose -f docker-compose-test.yaml down -v --rmi all

## build docker image
build-image:
	docker compose -f docker-compose-test.yaml build

## create environment (airflow container/operational events db/sql external db)
integration-environment:
	docker compose -f docker-compose-test.yaml up -d --wait

	echo "Initializing pg database"
	python ci_scripts/setup_pg_database.py

## tear down environment
integration-teardown:
	echo "Tearing down environment"
	docker-compose -f docker-compose-test.yaml down -v

	echo "Clearing caches"
	make clear
## run integration tests
integration-tests:
	make integration-environment

	echo "Running integration tests"
	pytest -v -s tests/integration --no-header -vv --alluredir=allure_results || (make integration-teardown && exit 1)
	make integration-teardown

## run unit tests
unit:
	make clear
	pytest -v -s tests/unit --no-header -vv --cov=dags --cov-report=term-missing
	coverage xml
	make clear


## run integrity tests
integrity:
	echo "Initialising integrity docker container"
	env CONFIGS_DIR=configs docker compose -f docker-compose-test.yaml up -d --wait

	echo "Running integrity tests"
	pytest -v -s tests/integrity --no-header -vv || (make integration-teardown && exit 1)
	make integration-teardown

check-for-import-error:
	echo "Initialising docker container"
	env CONFIGS_DIR=tests/resources/tmp_configs docker compose -f docker-compose-test.yaml up -d --wait

	echo "Running find import error tests"
	python ./scripts/find_import_errors.py

	echo "Deleting docker container"
	docker compose -f docker-compose-test.yaml down -v

	echo "Clearing caches"
	make clear

## linting checks and then run tests
tests: check build-image
	make unit
	make integration-tests


#################################################################################
# Self Documenting Commands                                                     #
#################################################################################
.DEFAULT_GOAL := help
# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
#   * save line in hold space
#   * purge line
#   * Loop:
#       * append newline + line to hold space
#       * go to next line
#       * if line starts with doc comment, strip comment character off and loop
#   * remove target prerequisites
#   * append hold space (+ newline) to line
#   * replace newline plus comments by `---`
#   * print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
