import os
from pathlib import Path
from src.logger import get_logger

logger = get_logger(__name__)

EXECUTE_ENV = os.getenv("EXECUTE_ENV")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", '')
PROJ_ID = os.getenv("PROJ_ID")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent 
GCP_KEY_PATH = GOOGLE_SERVICE_ACCOUNT_PATH