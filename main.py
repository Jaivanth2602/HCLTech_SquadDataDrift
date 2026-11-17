"""
Retail Sales Lakehouse: Main Entrypoint
Executes the Medallion Data Engineering Pipeline.
"""
from src.etl.run_pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline()
