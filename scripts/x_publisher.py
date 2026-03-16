# scripts/x_publisher.py
import json
import os
from pathlib import Path
from playwright.sync_api import sync_playwright


COOKIE_FILE = os.environ.get("X_COOKIES_FILE", "cookies.json")


def _normalize_cookies(raw_cookies):
    cookies = []
    for c in raw_cookies:
        cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c["domain"],
            "path": c.get("path", "/"),
            "secure": c.get("secure", False),
            "httpOnly": c.get("httpOnly", False),
        }

        if "expirationDate" in c:
            cookie["expires"] = int(c["expirationDate"])

        same_site = str(c.get("sameSite", "")).lower()
        if same_site == "lax":
            cookie["sameSite"] = "Lax"
        elif same_site in ("no_restriction", "none"):
            cookie["sameSite"] = "None"
        else:
            cookie["sameSite"] = "Lax"

        cookies.append(cookie)

    return cookies


def post_tweet(text: str, media_paths=None, headless: bool = True):
    if not text or not text.strip():
        raise ValueError("Tweet vacío")

    if len(text) > 280:
        raise ValueError(f"Tweet supera 280 caracteres: {len(text)}")

    media_paths = media_paths or []
    media_paths = [str(Path(p).resolve()) for p in media_paths]

    with open(COOKIE_FILE, "r", encoding="utf-8") as f:
        raw_cookies = json.load(f)

    cookies = _normalize_cookies(raw_cookies)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        context.add_cookies(cookies)

        page = context.new_page()
        page.goto("https://x.com/compose/tweet", wait_until="networkidle")

        textbox = page.locator('div[role="textbox"]').first
        textbox.wait_for(timeout=20000)
        textbox.click()
        textbox.fill(text)

        if media_paths:
            file_input = page.locator('input[type="file"]')
            file_input.set_input_files(media_paths)
            page.wait_for_timeout(5000)

        tweet_button = page.get_by_test_id("tweetButton")
        tweet_button.wait_for(timeout=20000)
        tweet_button.click()

        page.wait_for_timeout(5000)
        browser.close()