.PHONY: lint test fix

lint:
	ruff check kvstore.py test_kvstore.py

fix:
	ruff check --fix kvstore.py test_kvstore.py

test:
	pytest -v
