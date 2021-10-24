TOP_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
SHELL := /bin/bash

init:
	if [ ! -d "${TOP_DIR}/.venv/" ]; then \
		virtualenv -p python3 ${TOP_DIR}/.venv/; \
	fi && \
	source ${TOP_DIR}/.venv/bin/activate && \
	pip install -r ${TOP_DIR}/requirements.txt

build:	init

tests:	build
	cd ${TOP_DIR} && \
	source ${TOP_DIR}/.venv/bin/activate && \
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$[TOP_DIR} coverage run -m unittest discover --verbose -t ${TOP_DIR} -s ${TOP_DIR}/tests --pattern '*_test.py'
