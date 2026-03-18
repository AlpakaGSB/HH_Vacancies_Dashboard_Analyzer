# scripts/enricher.py
import time
import json
import requests
import random
from pathlib import Path
from datetime import datetime

from scripts.utils import logger, sleep_with_jitter, load_token, save_json

# ========================= НАСТРОЙКИ =========================
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw_lists"
ENRICHED_DIR = BASE_DIR / "data" / "enriched"

MAX_IT_PER_RUN = 140
MAX_COURIER_PER_RUN = 80


# ============================================================

def get_latest_raw_file() -> Path | None:
    files = sorted(RAW_DIR.glob("raw_list_*.json"), reverse=True)
    return files[0] if files else None


def load_raw_data(path: Path) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Не удалось загрузить raw-файл: {e}")
        return None


def get_random_unenriched_ids(data: dict, group: str, max_count: int) -> list[str]:
    """Возвращает случайные необогащённые id из указанной группы"""
    candidates = [
        vac["id"] for vac in data.get(group, [])
        if "full" not in vac
    ]
    random.shuffle(candidates)  # ← вот здесь перемешиваем
    return candidates[:max_count]


def enrich_vacancy(vac_id: str, token: str) -> dict | None:
    url = f"https://api.hh.ru/vacancies/{vac_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "HH_Vacancies_Dashboard_Analyzer/1.0"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            logger.warning(f"{vac_id} → {r.status_code}")
            return None
    except Exception as e:
        logger.error(f"{vac_id} → ошибка: {e}")
        return None


def main():
    token = load_token()
    if not token:
        return

    raw_path = get_latest_raw_file()
    if not raw_path:
        logger.error("Raw-файлы не найдены")
        return

    logger.info(f"Используем файл: {raw_path.name}")
    raw_data = load_raw_data(raw_path)
    if not raw_data:
        return

    # === Случайный отбор ===
    it_ids = get_random_unenriched_ids(raw_data, "it", MAX_IT_PER_RUN)
    courier_ids = get_random_unenriched_ids(raw_data, "courier", MAX_COURIER_PER_RUN)

    logger.info(f"Выбрано для обогащения: {len(it_ids)} IT + {len(courier_ids)} Курьеров")

    all_ids = it_ids + courier_ids
    random.shuffle(all_ids)  # перемешиваем порядок запросов

    enriched_items = []
    start_time = time.monotonic()

    for i, vac_id in enumerate(all_ids, 1):
        # Прогресс в консоль (без logging)
        print(f"[{i:3d}/{len(all_ids)}] {vac_id} ... ", end="", flush=True)

        full = enrich_vacancy(vac_id, token)

        if full:
            enriched_items.append({"id": vac_id, "full": full})
            print("OK")
        else:
            print("—")

        sleep_with_jitter(0.45, 0.35)

    elapsed = time.monotonic() - start_time
    logger.info(f"Завершено за {elapsed:.1f} с. Успешно обогащено: {len(enriched_items)}")

    # Сохраняем результат
    if enriched_items:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        out_path = ENRICHED_DIR / f"enriched_{timestamp}.json"

        result = {
            "enriched_at": datetime.now().isoformat(),
            "total": len(enriched_items),
            "it_count": len([x for x in it_ids if x in [e["id"] for e in enriched_items]]),
            "courier_count": len([x for x in courier_ids if x in [e["id"] for e in enriched_items]]),
            "items": enriched_items
        }
        save_json(result, out_path)
        logger.info(f"Сохранено {len(enriched_items)} обогащённых вакансий")
    else:
        logger.info("Новых вакансий для обогащения не найдено")


if __name__ == "__main__":
    main()