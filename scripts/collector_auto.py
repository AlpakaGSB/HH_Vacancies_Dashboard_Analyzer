# scripts/collector_smart.py
import requests
import yaml
from pathlib import Path
from datetime import datetime
import random
import os

from scripts.utils import logger, sleep_with_jitter, save_json

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

with open(CONFIG_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

USER_AGENT = f"{config['project']['name']}/{config['project']['version']} (pet-project)"
HEADERS = {"User-Agent": USER_AGENT, "HH-User-Agent": USER_AGENT}

PER_PAGE = config["collection"]["per_page"]
MAX_PAGES = config["collection"]["max_pages_per_search"]
AREA = config["collection"]["area"]

IT_QUERIES = config["professions"]["it"]["queries"]
COURIER_QUERIES = config["professions"]["courier"]["queries"]

DATA_DIR = BASE_DIR / "data" / "raw_lists"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def generate_combinations(queries):
    combinations = []
    for q in queries:
        base = {"text": q, "area": AREA}
        combinations.append(base)
        combinations.append({**base, "schedule": "remote"})
        combinations.append({**base, "only_with_salary": "true"})
        combinations.append({**base, "experience": "noExperience"})
        combinations.append({**base, "experience": "between1And3"})
    logger.info(f"Сгенерировано {len(combinations)} комбинаций")
    return combinations


def collect_for_group(combinations, group_name):
    all_vacancies = []
    seen_ids = set()

    logger.info(f"Начало сбора: {group_name}")

    for idx, params in enumerate(combinations, 1):
        logger.info(f"[{idx}/{len(combinations)}] Поиск: {params.get('text')} {params.get('schedule', '')} {params.get('experience', '')}")

        page = 0
        while page < MAX_PAGES:
            params_page = {**params, "per_page": PER_PAGE, "page": page, "order_by": "publication_time"}

            try:
                r = requests.get("https://api.hh.ru/vacancies", headers=HEADERS, params=params_page, timeout=10)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Ошибка запроса: {e}")
                break

            items = r.json().get("items", [])

            added = 0
            for item in items:
                vid = item["id"]
                if vid not in seen_ids:
                    seen_ids.add(vid)
                    all_vacancies.append(item)
                    added += 1

            logger.info(f"  Страница {page:2d} | найдено {len(items)}, новых {added}, всего {len(all_vacancies)}")

            if len(items) < PER_PAGE:
                break

            page += 1
            sleep_with_jitter()

    logger.info(f"{group_name} завершено: {len(all_vacancies)} уникальных")
    return all_vacancies


def main():
    it_combs = generate_combinations(IT_QUERIES)
    courier_combs = generate_combinations(COURIER_QUERIES)

    it_vacs = collect_for_group(it_combs, "IT")
    courier_vacs = collect_for_group(courier_combs, "Курьеры")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = DATA_DIR / f"raw_list_{timestamp}.json"

    result = {
        "collected_at": datetime.now().isoformat(),
        "total_it": len(it_vacs),
        "total_courier": len(courier_vacs),
        "it": it_vacs,
        "courier": courier_vacs
    }

    save_json(result, filename)


if __name__ == "__main__":
    main()