from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time

BASE_URL = "https://www.emag.ro/telefoane-mobile"
MAX_PAGES = 1


def clean_price(text):
    if not text:
        return None
    return int(re.sub(r"[^\d]", "", text))


def scrape_emag_phones():
    results = []
    scrape_date = datetime.utcnow().date().isoformat()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for page_no in range(1, MAX_PAGES + 1):
            url = f"{BASE_URL}/c" if page_no == 1 else f"{BASE_URL}/p{page_no}/c"
            print(f"Scraping page {page_no}")

            page.goto(url, timeout=60000)
            page.wait_for_selector("div.card-item")

            soup = BeautifulSoup(page.content(), "html.parser")
            cards = soup.select("div.card-item")

            for card in cards:
                title = card.select_one("a.card-v2-title")
                if not title:
                    continue

                name = title.get_text(strip=True)
                link = title["href"]

                price_el = card.select_one("p.product-new-price")
                price = clean_price(price_el.text) if price_el else None

                results.append({
                    "name": name,
                    "brand": name.split(),
                    "price_ron": price,
                    "url": link,
                    "scrape_date": scrape_date,
                    "page": page_no
                })

            time.sleep(2)

        browser.close()

    return results

products = scrape_emag_phones()
len(products)
type(products)
print(products)

for product in range(len(products)):
    print(products[product]) 
