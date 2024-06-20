.PHONY: all seed run test docs serve compare connect artifacts

all: init seed run test docs artifacts

develop: init seed run test compare

# Set python path for all scripts
export PYTHONPATH := $(shell pwd)/..

# Helper to connect to output duckdb with original schema loaded in.
connect:
	duckdb output/output.duckdb --cmd "attach 'input/input.duckdb' as raw (READ_ONLY);"

clean:
	rm -f output/*.duckdb
	rm -f output/*.csv
	rm -f input/*.duckdb
	rm -f input/*.json

init:
	python init.py ${config_url}

seed:
	dbt seed --full-refresh --profiles-dir .

run:
	dbt run --profiles-dir .

test:
	dbt test --profiles-dir .

compare:
	python ../framework/app.py compare_output

compare_mapping_suggester:
	python ../framework/app.py compare_mapping_suggester

pull:
ifdef version_id
	python ../framework/app.py pull --version_id $(version_id)
else
	python ../framework/app.py pull
endif

push:
	python ../framework/app.py push

versions:
	python ../framework/app.py versions

docs:
	dbt docs generate --profiles-dir .

serve:
	dbt docs serve --profiles-dir .

artifacts:
	python artifacts.py ${task_id} ${config_url}

dependencies:
	python ../framework/business/dependency_finder/__init__.py

trace:
	streamlit run ../framework/business/trace/main.py

document_viewer:
	streamlit run ../framework/business/document_viewer/main.py