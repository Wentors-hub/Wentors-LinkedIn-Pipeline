import asyncio
import json
from playwright.async_api import async_playwright

COOKIE_FILE = "linkedin_cookies.json"
LINKEDIN_LOGIN_URL = "https://www.linkedin.com/login"

async def login_and_save_cookies():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # See the browser so you can log in manually
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage'
            ]
        )

        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/New_York"
        )

        page = await context.new_page()

        print(f"🌐 Navigating to LinkedIn login: {LINKEDIN_LOGIN_URL}")
        await page.goto(LINKEDIN_LOGIN_URL, wait_until="networkidle")

        print("\n🔑 Please log in to LinkedIn in the opened browser window.")
        print("Once you see your LinkedIn feed, press ENTER here to save cookies.\n")
        input("Press ENTER when you are logged in...")

        # Save cookies
        cookies = await context.cookies()
        with open(COOKIE_FILE, "w") as f:
            json.dump(cookies, f)

        print(f"✅ Cookies saved to {COOKIE_FILE}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(login_and_save_cookies())
