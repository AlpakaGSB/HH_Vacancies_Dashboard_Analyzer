# scripts/utils.py
import json
import time
import random
import logging
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def load_token() -> Optional[str]:
    token = os.getenv("HH_ACCESS_TOKEN")
    if not token:
        logger.error("HH_ACCESS_TOKEN не найден в .env")
    return token

def sleep_with_jitter(base: float = 0.45, jitter: float = 0.35):
    time.sleep(base + random.uniform(-jitter, jitter))

def save_json(data: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Сохранено: {path}")