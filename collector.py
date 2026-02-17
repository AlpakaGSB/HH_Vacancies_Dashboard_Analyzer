import requests
import json
import time
import random
from datetime import datetime
from pathlib import Path


DATA_DIR = Path("data/raw_lists")
DATA_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = "HH_Vacancies_Dashboard_Analyzer/1.0 (inside12111@gmail.com - pet project)"

HEADERS = {
    "User-Agent": USER_AGENT,
    "HH-User-Agent": USER_AGENT
}

PER_PAGE = 100
MAX_PAGES_PER_SEARCH = 20


IT_QUERIES = [
    "data analyst",
    "аналитик данных",
    "python developer",
    "data scientist",
    "backend developer",
    "frontend developer",
]

COURIER_QUERIES = [
    "курьер",
    "курьер пеший",
    "курьер на авто",
    "курьер на электросамокате",
    "курьер на велосипеде",
]


def generate_combinations(queries):
    combinations = []
    for query in queries:
        combinations.append({"text": query, "area": "1"})
        combinations.append({"text": query, "area": "1", "schedule": "remote"})
        combinations.append({"text": query, "area": "1", "only_with_salary": "true"})
        combinations.append({"text": query, "area": "1", "experience": "noExperience"})
        combinations.append({"text": query, "area": "1", "experience": "between1And3"})
    return combinations


def collect_for_profession(combinations, profession_name):
    all_vacancies = []
    seen_ids = set()

    print(f"\n=== Сбор вакансий: {profession_name} ===")

    for idx, params in enumerate(combinations, 1):
        print(f"[{idx:2d}/{len(combinations)}] Поиск: {params.get('text')}")

        page = 0
        while page < MAX_PAGES_PER_SEARCH:
            request_params = {
                **params,
                "per_page": PER_PAGE,
                "page": page,
                "order_by": "publication_time"
            }

            r = requests.get("https://api.hh.ru/vacancies", headers=HEADERS, params=request_params)

            if r.status_code != 200:
                print(f"   Ошибка {r.status_code}")
                break

            items = r.json().get("items", [])

            for item in items:
                vac_id = item["id"]
                if vac_id not in seen_ids:
                    seen_ids.add(vac_id)
                    all_vacancies.append(item)

            print(f"   Страница {page:2d}: +{len(items)} найдено, всего {len(all_vacancies)}")

            if len(items) < PER_PAGE:
                break

            page += 1
            time.sleep(random.uniform(0.4, 1.2))

    print(f"Собрано {len(all_vacancies)} уникальных вакансий для {profession_name}")
    return all_vacancies


def save_raw_data(it_vacs, courier_vacs):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = DATA_DIR / f"raw_list_{timestamp}.json"

    result = {
        "collected_at": datetime.now().isoformat(),
        "total_it": len(it_vacs),
        "total_courier": len(courier_vacs),
        "it": it_vacs,
        "courier": courier_vacs
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Данные сохранены: {filename}")


if __name__ == "__main__":
    print("=== HH Vacancies Collector (IT + Курьеры) ===\n")

    it_combinations = generate_combinations(IT_QUERIES)
    courier_combinations = generate_combinations(COURIER_QUERIES)

    it_vacancies = collect_for_profession(it_combinations, "IT")
    courier_vacancies = collect_for_profession(courier_combinations, "Курьеры")

    save_raw_data(it_vacancies, courier_vacancies)

    print("\nГотово!")