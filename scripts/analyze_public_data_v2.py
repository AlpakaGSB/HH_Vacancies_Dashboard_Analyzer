# scripts/analyze_public_data_v2.py
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import sqlite3
import re
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud

from scripts.utils import logger

# ────────────────────────────────────────────────
#  НАСТРОЙКИ
# ────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw_lists"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DB_PATH = PROCESSED_DIR / "hh_public.db"

PROCESSED_DIR.mkdir(exist_ok=True)

def get_latest_raw_file() -> Path | None:
    files = sorted(RAW_DIR.glob("raw_list_*.json"), reverse=True)
    return files[0] if files else None


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text


def extract_words(texts: list[str]) -> list[str]:
    words = []
    for t in texts:
        cleaned = clean_text(t)
        words.extend(cleaned.split())
    return words


def save_wordcloud(words: list[str], title: str, filename: Path):
    text = " ".join(words)
    if not text.strip():
        logger.warning(f"Нет слов для wordcloud: {title}")
        return

    wordcloud = WordCloud(width=800, height=400, background_color="white", max_words=100).generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(title)
    plt.savefig(filename, bbox_inches="tight")
    plt.close()
    logger.info(f"Wordcloud сохранён: {filename}")


def main():
    raw_path = get_latest_raw_file()
    if not raw_path:
        logger.error("Raw-файлы не найдены")
        return

    logger.info(f"Анализируем: {raw_path.name}")

    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for group in ("it", "courier"):
        for vac in data.get(group, []):
            salary = vac.get("salary")
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
    logger.info(f"Всего вакансий: {len(df)}")

    # Сохраняем в SQLite
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("public_vacancies", conn, if_exists="replace", index=False)
    logger.info(f"Таблица сохранена в SQLite: {DB_PATH}")

    # Метрики
    logger.info("\n=== Основные метрики ===")
    logger.info(f"Количество: {df['group'].value_counts().to_dict()}")
    logger.info(f"% с ЗП:\n{df.groupby('group')['has_salary'].mean() * 100}")
    logger.info(f"% удалёнки:\n{df.groupby('group')['schedule_id'].apply(lambda x: (x == 'remote').mean() * 100)}")
    logger.info(f"Распределение опыта (%):\n{df.groupby(['group', 'experience_id']).size().unstack(fill_value=0).pipe(lambda x: x.div(x.sum(1), 0) * 100)}")

    # Топ-компании
    top_comp = df.groupby("group")["employer"].value_counts().groupby(level=0).head(10)
    logger.info(f"\nТоп-10 компаний:\n{top_comp}")

    # Топ-слова
    it_snippets = df[df["group"] == "it"][["snippet_req", "snippet_resp"]].fillna("").agg(" ".join, axis=1)
    courier_snippets = df[df["group"] == "courier"][["snippet_req", "snippet_resp"]].fillna("").agg(" ".join, axis=1)

    it_words = Counter(extract_words(it_snippets))
    courier_words = Counter(extract_words(courier_snippets))

    logger.info(f"\nТоп-20 слов IT:\n{it_words.most_common(20)}")
    logger.info(f"Топ-20 слов Курьеры:\n{courier_words.most_common(20)}")

    # Word Cloud
    save_wordcloud(extract_words(it_snippets), "IT — Топ слова в описании", PROCESSED_DIR / "wordcloud_it.png")
    save_wordcloud(extract_words(courier_snippets), "Курьеры — Топ слова в описании", PROCESSED_DIR / "wordcloud_courier.png")

    conn.close()
    logger.info("Анализ завершён!")


if __name__ == "__main__":
    main()