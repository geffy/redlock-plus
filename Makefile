.PHONY: docs

lint:
	black redlock_plus.py tests
	mypy redlock_plus.py
	flake8 redlock_plus.py tests
	pylint redlock_plus.py

test:
	py.test

test_fast:
	py.test -m "not slow"

coverage:
	pytest --cov=redlock_plus
	coverage report -m
	coverage html

docs:
	sphinx-build -b html docs/source docs/build
