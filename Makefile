help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "test - run tests quickly with the default Python"
	@echo "build - package"
	@echo "run - activates the created virtualenv"

all: default

default: clean deps test build run

.venv:
	if [ ! -e ".venv/bin/activate_this.py" ] ; then virtualenv --clear .venv ; fi

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '*.egg-info' -exec rm -rf {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

deps: .venv
	. .venv/bin/activate && pip install -U -r requirements.txt -t ./src/libs
	. .venv/bin/activate && pip install -U -r requirements.txt 

test:
	. .venv/bin/activate && pytest ./tests/* 

build: clean
	. .venv/bin/activate && python setup.py bdist_egg

run: 
	. .venv/bin/activate 