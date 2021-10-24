TOP_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
SHELL := /bin/bash

init:
	if [ ! -d "${TOP_DIR}/.venv/" ]; then \
		virtualenv --version >/dev/null 2>&1 || pip install virtualenv; \
		python3 -B -m virtualenv -p python3 ${TOP_DIR}/.venv/; \
	fi && \
	source ${TOP_DIR}/.venv/bin/activate && \
	pip install -r ${TOP_DIR}/requirements.txt -r ${TOP_DIR}/requirements-dev.txt

build:	init

tests:	build
	cd ${TOP_DIR} && \
	source ${TOP_DIR}/.venv/bin/activate && \
	PYTHONPATH=$[TOP_DIR} python3 -B -m coverage run -m unittest discover --verbose -t ${TOP_DIR} -s ${TOP_DIR}/tests --pattern '*_test.py'

start-localstack:
	cd ${TOP_DIR} && \
	docker-compose -f ${TOP_DIR}/localstack/docker-compose.yml up -d

stop-localstack:
	cd ${TOP_DIR} && \
	docker-compose -f ${TOP_DIR}/localstack/docker-compose.yml down

integration-tests:	build
	cd ${TOP_DIR} && \
	source ${TOP_DIR}/.venv/bin/activate && \
	PYTHONPATH=$[TOP_DIR} python3 -B -m coverage run -m unittest discover --verbose -t ${TOP_DIR} -s ${TOP_DIR}/integration_tests --pattern '*_test.py'

dist:   tests
	source ${TOP_DIR}/.venv/bin/activate && \
	python3 -B ${TOP_DIR}/setup.py sdist bdist_wheel

clean:
	cd ${TOP_DIR} && \
	rm -rf .coverage build/ dist/ *.egg-info/

upload: clean dist
	cd ${TOP_DIR} && \
	source ${TOP_DIR}/.venv/bin/activate && \
	twine upload dist/*

