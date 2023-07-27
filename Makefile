test:
	pytest .

fmt:
	pip install -q black
	black .
