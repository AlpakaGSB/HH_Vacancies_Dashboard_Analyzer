# scripts/test_api.py
# Тестовый скрипт для проверки токена и доступа к API hh.ru
# Запускай: python -m scripts.test_api

import requests
import json
import os
from pathlib import Path

from scripts.utils import logger

# ────────────────────────────────────────────────
#  НАСТРОЙКИ
# ────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parents[1]
TOKENS_PATH = BASE_DIR / "tokens.json"   # если используешь старый способ
ENV_TOKEN = os.getenv("HH_ACCESS_TOKEN") # основной способ через .env

# Тестовый ID вакансии
TEST_VACANCY_ID = "130543348"  # ← замени на реальный из твоего последнего raw-файла

HEADERS_PUBLIC = {
    "User-Agent": "HH_Vacancies_Dashboard_Analyzer/test (pet-project)"
}

def get_token() -> str | None:
    # 1. Пробуем из .env (основной способ)
    token = os.getenv("HH_ACCESS_TOKEN")
    if token:
        logger.info("Токен взят из переменной окружения (.env)")
        return token

    # 2. Пробуем из tokens.json (старый способ)
    if TOKENS_PATH.is_file():
        try:
            with open(TOKENS_PATH, encoding="utf-8") as f:
                data = json.load(f)
                token = data.get("access_token")
                if token:
                    logger.info("Токен взят из tokens.json")
                    return token
        except Exception as e:
            logger.error(f"Ошибка чтения tokens.json: {e}")

    logger.error("Токен не найден ни в .env, ни в tokens.json")
    return None


def test_public_search():
    """Проверка публичного поиска без токена"""
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "python developer",
        "area": "1",
        "per_page": 1
    }

    try:
        r = requests.get(url, headers=HEADERS_PUBLIC, params=params, timeout=5)
        if r.status_code == 200:
            data = r.json()
            logger.info("Публичный поиск работает ✓")
            logger.info(f"Найдено вакансий: {data.get('found')}")
            if data.get("items"):
                logger.info(f"Пример вакансии: {data['items'][0]['name']} (id: {data['items'][0]['id']})")
            return True
        else:
            logger.error(f"Публичный поиск упал: {r.status_code} — {r.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при публичном поиске: {e}")
        return False


def test_vacancy_detail(token: str):
    """Проверка доступа к деталям вакансии с токеном"""
    if not token:
        logger.warning("Токен не передан → пропускаем тест деталей")
        return

    url = f"https://api.hh.ru/vacancies/{TEST_VACANCY_ID}"
    headers = {
        **HEADERS_PUBLIC,
        "Authorization": f"Bearer {token}"
    }

    try:
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json()
            logger.info("Доступ к деталям вакансии работает ✓")
            logger.info(f"Название: {data.get('name')}")
            logger.info(f"Компания: {data.get('employer', {}).get('name')}")
            salary = data.get("salary")
            if salary:
                logger.info(f"Зарплата: от {salary.get('from')} до {salary.get('to')} {salary.get('currency')}")
            skills = data.get("key_skills", [])
            if skills:
                logger.info(f"Навыки ({len(skills)}): {', '.join(s['name'] for s in skills[:5])}...")
            return True
        else:
            logger.warning(f"Детали вакансии недоступны: {r.status_code} — {r.text[:300]}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при запросе деталей: {e}")
        return False


def main():
    logger.info("=== Тест API hh.ru ===")

    # 1. Проверка публичного поиска (должен работать всегда)
    public_ok = test_public_search()

    # 2. Проверка токена и деталей вакансии
    token = get_token()
    if token:
        logger.info(f"Токен найден (первые 10 символов): {token[:10]}...")
        detail_ok = test_vacancy_detail(token)
    else:
        detail_ok = False

    logger.info("\n=== Итог теста ===")
    logger.info(f"Публичный поиск: {'OK' if public_ok else 'ОШИБКА'}")
    logger.info(f"Детали вакансий (с токеном): {'OK' if detail_ok else 'НЕДОСТУПНО (скорее всего 403)'}")

    if not detail_ok and token:
        logger.warning(
            "Вероятно, у тебя applicant-токен. С 15 декабря 2025 полные данные вакансий доступны только для employer-токенов.\n"
            "Рекомендация: подай новую заявку на dev.hh.ru как employer-приложение."
        )


if __name__ == "__main__":
    main()