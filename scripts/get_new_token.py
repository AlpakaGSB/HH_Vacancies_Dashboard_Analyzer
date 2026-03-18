# scripts/get_new_token.py
import requests
import webbrowser
from urllib.parse import urlencode
import json
from datetime import datetime

CLIENT_ID = "TGUKNF3PHGIF3VOQ3TR48P3H366LNJOM6SRU0GC0M70JAHA59C49C50U0RP4UACI"
CLIENT_SECRET = "GJOM6MKA3SR7806CATQ8KJPRSO09CQ88OUU5QOJTTU9RCIKNDROEPO1LRUB1D6KE"
REDIRECT_URI = "http://localhost:8080/callback"

AUTH_URL = "https://hh.ru/oauth/authorize"
TOKEN_URL = "https://api.hh.ru/token"

def main():
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI
    }
    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    print("Открываю браузер для авторизации...")
    webbrowser.open(auth_url)

    print("После авторизации вставь сюда URL из адресной строки браузера (или просто нажми Enter, если используешь Flask-сервер):")
    callback_url = input("> ")

    # Извлекаем code из URL
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(callback_url)
    code = parse_qs(parsed.query).get("code", [None])[0]

    if not code:
        print("Code не найден в URL")
        return

    response = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI
    })

    if response.status_code == 200:
        tokens = response.json()
        tokens["obtained_at"] = datetime.now().isoformat()
        tokens["client_id"] = CLIENT_ID  # для удобства

        with open("tokens_new.json", "w", encoding="utf-8") as f:
            json.dump(tokens, f, ensure_ascii=False, indent=2)

        print("Новый токен успешно сохранён в tokens_new.json")
        print(f"Access token: {tokens['access_token'][:20]}...")
        print(f"Refresh token: {tokens.get('refresh_token')}")
    else:
        print(f"Ошибка: {response.status_code} — {response.text}")

if __name__ == "__main__":
    main()