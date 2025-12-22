# scripts/x_publisher.py
import json
from playwright.sync_api import sync_playwright
import os

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

        same_site = c.get("sameSite", "").lower()
        if same_site == "lax":
            cookie["sameSite"] = "Lax"
        elif same_site in ("no_restriction", "none"):
            cookie["sameSite"] = "None"
        else:
            cookie["sameSite"] = "Lax"

        cookies.append(cookie)

    return cookies


def post_tweet(text: str, headless: bool = True):
    if not text or not text.strip():
        raise ValueError("Tweet vacío")

    if len(text) > 280:
        raise ValueError("Tweet supera 280 caracteres")

    with open(COOKIE_FILE, "r", encoding="utf-8") as f:
        raw_cookies = json.load(f)

    cookies = _normalize_cookies(raw_cookies)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        context.add_cookies(cookies)

        page = context.new_page()
        page.goto("https://x.com/compose/tweet", wait_until="domcontentloaded")

        textbox = page.get_by_role("textbox")
        textbox.wait_for(timeout=15000)
        textbox.click()
        
        # Inyectar texto de forma compatible con React (X)
        page.evaluate(
            """
            (text) => {
                const box = document.querySelector('div[role="textbox"]');
                box.focus();
                box.innerText = text;
                box.dispatchEvent(new InputEvent('input', { bubbles: true }));
            }
            """,
            text
        )
        
        # Forzar blur
        page.keyboard.press("Tab")
        
        tweet_button = page.get_by_test_id("tweetButton")
        
        # Forzar escritura en TODOS los textbox activos (X a veces duplica)
	page.evaluate(
    	"""
    	(text) => {
        	const boxes = document.querySelectorAll('div[role="textbox"]');
        	boxes.forEach(box => {
            	box.focus();
            	box.innerText = text;
            	box.dispatchEvent(new InputEvent('input', { bubbles: true }));
        	});
    	}
    	""",
    	text
	)

	# Forzar blur global
	page.keyboard.press("Tab")

	tweet_button = page.get_by_test_id("tweetButton")

	# Esperar a que exista y click forzado (JS-level)
	tweet_button.wait_for(timeout=20000)

	page.evaluate(
    	"""
    	() => {
        	const btn = document.querySelector('[data-testid="tweetButton"]');
        	btn.click();
    	}
    	"""
	)

        
        tweet_button.click()



        page.wait_for_timeout(3000)
        browser.close()
