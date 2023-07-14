# install Poetry and Python dependencies
init:
	curl -sSL https://install.python-poetry.org | python -
	poetry install

# run the pipeline 
run:
	poetry run python src/pipeline.py --reference_date $(ref_dt)