import os
from pathlib import Path
from src.logger import get_logger
import dotenv
logger = get_logger(__name__)
dotenv.load_dotenv()

PROJ_ID = os.getenv("PROJ_ID")
EXECUTE_ENV = os.getenv("EXECUTE_ENV")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", '')
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", '')
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", '')
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent 
GCP_KEY_PATH = GOOGLE_SERVICE_ACCOUNT_PATH