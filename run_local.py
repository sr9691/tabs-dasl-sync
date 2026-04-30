"""Local driver: invokes main.run() so we don't need to host the Cloud Function."""
import logging, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from main import run, GCP_PROJECT_ID

logging.getLogger().setLevel(logging.INFO)
summary = run(project_id=GCP_PROJECT_ID, dataset=os.environ.get("BQ_DATASET", "dasl"))
print("SUMMARY:", summary)
