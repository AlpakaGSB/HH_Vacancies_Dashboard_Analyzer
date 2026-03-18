# scripts/analyze_public_data.py
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from collections import Counter
import re

from scripts.utils import logger

# ────────────────────────────────────────────────
#  НАСТРОЙКИ
# ────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw_lists"

def get_latest_raw_file() -> Path | None:
    files = sorted(RAW_DIR.glob("raw_list_*.json"), reverse=True)
    return files[0] if files else None

def clean_text(text: str | None) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)  # убираем пунктуацию
    return text

def extract_words(texts: list[str]) -> list[str]:
    words = []
    for t in texts:
        cleaned = clean_text(t)
        words.extend(cleaned.split())
    return words

def main():
    raw_path = get_latest_raw_file()
    if not raw_path:
        logger.error("Raw-файлы не найдены")
        return

    logger.info(f"Анализируем файл: {raw_path.name}")

    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)

    # Преобразуем в DataFrame
    rows = []
    for group in ("it", "courier"):
        for vac in data.get(group, []):
            salary = vac.get("salary")  # может быть None или dict
            rows.append({
                "group": group,
                "vacancy_id": vac["id"],
                "name": vac.get("name"),
                "employer": vac.get("employer", {}).get("name"),
                "experience_id": vac.get("experience", {}).get("id"),
                "schedule_id": vac.get("schedule", {}).get("id"),
                "has_salary": salary is not None,
                "salary_from": salary.get("from") if salary else None,
                "salary_to": salary.get("to") if salary else None,
                "currency": salary.get("currency") if salary else None,
                "snippet_req": vac.get("snippet", {}).get("requirement"),
                "snippet_resp": vac.get("snippet", {}).get("responsibility"),
                "published_at": vac.get("published_at")
            })

    df = pd.DataFrame(rows)
    logger.info(f"Всего строк в DataFrame: {len(df)}")

    # 1. Количество вакансий
    counts = df["group"].value_counts()
    logger.info("\nКоличество вакансий:")
    logger.info(counts)

    # 2. % с указанной зарплатой
    salary_pct = df.groupby("group")["has_salary"].mean() * 100
    logger.info("\n% вакансий с указанной зарплатой:")
    logger.info(salary_pct.round(1))

    # 3. % удалёнки
    remote_pct = df.groupby("group")["schedule_id"].apply(lambda x: (x == "remote").mean() * 100)
    logger.info("\n% удалёнки:")
    logger.info(remote_pct.round(1))

    # 4. Распределение по опыту (%)
    exp_dist = df.groupby(["group", "experience_id"]).size().unstack(fill_value=0)
    exp_dist_pct = exp_dist.div(exp_dist.sum(axis=1), axis=0) * 100
    logger.info("\nРаспределение по опыту (%):")
    logger.info(exp_dist_pct.round(1))

    # 5. Топ-10 компаний
    top_comp = df.groupby("group")["employer"].value_counts().groupby(level=0).head(10)
    logger.info("\nТоп-10 компаний:")
    logger.info(top_comp)

    # 6. Топ-слова в snippet (требования + обязанности)
    all_snippets = df["snippet_req"].fillna("") + " " + df["snippet_resp"].fillna("")
    it_snippets = df[df["group"] == "it"]["snippet_req"].fillna("") + " " + df[df["group"] == "it"]["snippet_resp"].fillna("")
    courier_snippets = df[df["group"] == "courier"]["snippet_req"].fillna("") + " " + df[df["group"] == "courier"]["snippet_resp"].fillna("")

    it_words = Counter(extract_words(it_snippets))
    courier_words = Counter(extract_words(courier_snippets))

    logger.info("\nТоп-20 слов в IT (требования + обязанности):")
    logger.info(it_words.most_common(20))

    logger.info("\nТоп-20 слов в Курьерах:")
    logger.info(courier_words.most_common(20))

    # Сохраняем в CSV
    csv_path = BASE_DIR / "data" / "processed" / f"public_metrics_{datetime.now():%Y-%m-%d_%H-%M}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"\nДанные сохранены в: {csv_path}")

if __name__ == "__main__":
    main()