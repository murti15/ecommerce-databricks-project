# Ecommerce Data Pipeline

## Tech Stack
- PySpark
- Delta Lake
- GitHub Actions CI

## Architecture
Source → Bronze → Silver → Gold

## Run Locally
pip install pyspark delta-spark
python pipelines/ecommerce_pipeline.py

## CI/CD
- GitHub Actions validates PySpark pipelines on every PR
