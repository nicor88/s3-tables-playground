clean:
	rm -rf .venv;

init:
	uv venv;

setup:
	uv sync --locked;

setup-all: clean init setup

clean-iceberg:
	rm -rf iceberg_warehouse;
