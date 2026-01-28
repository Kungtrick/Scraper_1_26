import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
import os
from datetime import datetime
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================
# CONFIGURATION
# =============================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
OUTPUT_DIR = "jumia_results"

MAX_WORKERS = 8        # Safe parallel PDP requests
PAGE_DELAY = 0.8       # Delay between category pages (human-like)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================
# SESSION (SPEED BOOST)
# =============================
session = requests.Session()
session.headers.update(HEADERS)

# =============================
# HELPERS
# =============================
def get_base_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"

def get_soup(url):
    res = session.get(url, timeout=25)
    res.raise_for_status()
    return BeautifulSoup(res.text, "lxml")

# =============================
# LISTING PAGE
# =============================
def extract_product_links(listing_url, base_url):
    soup = get_soup(listing_url)

    items = soup.select("a.core[href*='.html']")
    links = []

    for a in items:
        href = a.get("href")
        if href:
            links.append(urljoin(base_url, href))

    # De-duplicate while preserving order
    return list(dict.fromkeys(links))

# =============================
# PDP SCRAPER
# =============================
def scrape_product_details(product_url):
    soup = get_soup(product_url)

    product_name = soup.select_one("h1.-fs20")
    product_name = product_name.get_text(strip=True) if product_name else None

    brand = soup.select_one("div.-pvxs a._more")
    brand = brand.get_text(strip=True) if brand else None

    category = " > ".join(
        [a.get_text(strip=True) for a in soup.select("div.brcbs a.cbs")]
    )

    image_tag = soup.select_one("a.itm img")
    image_url = (
        image_tag.get("data-src") or image_tag.get("src")
        if image_tag else None
    )

    tag = soup.select_one("a.bdg")
    tag = tag.get_text(strip=True) if tag else None

    sku = None
    for li in soup.select("li"):
        text = li.get_text(strip=True)
        if text.upper().startswith("SKU"):
            sku = text.replace("SKU:", "").strip()
            break

    seller = soup.select_one("p.-m.-pbs")
    seller = seller.get_text(strip=True) if seller else None

    return {
        "Product URL": product_url,
        "Category Path": category,
        "Product Name": product_name,
        "Brand": brand,
        "Image URL": image_url,
        "Tag": tag,
        "SKU": sku,
        "Seller Name": seller
    }

# =============================
# MAIN SCRAPER (FAST + SAFE)
# =============================
def scrape_jumia_category(start_url):
    all_products = []
    base_url = get_base_url(start_url)
    page = 1

    while True:
        separator = "&" if "?" in start_url else "?"
        page_url = f"{start_url}{separator}page={page}"

        print(f"\n📄 Scraping Page {page}: {page_url}")

        try:
            product_links = extract_product_links(page_url, base_url)
            print(f"🔗 Found {len(product_links)} products")

            if not product_links:
                print("🛑 No more products — stopping pagination.")
                break

            # ---- PARALLEL PDP SCRAPING ----
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_url = {
                    executor.submit(scrape_product_details, link): link
                    for link in product_links
                }

                for i, future in enumerate(as_completed(future_to_url), start=1):
                    link = future_to_url[future]
                    try:
                        all_products.append(future.result())
                        print(f"  ⚡ [{i}/{len(product_links)}] scraped")
                    except Exception as e:
                        print(f"  ❌ PDP failed: {link} → {e}")

            page += 1
            time.sleep(PAGE_DELAY)

        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            break

    return pd.DataFrame(all_products)

# =============================
# FILE NAMING
# =============================
def generate_unique_filename(base_name="jumia_products"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r'[^A-Za-z0-9_-]+', '_', base_name)
    return os.path.join(OUTPUT_DIR, f"{safe_name}_{timestamp}.csv")

# =============================
# RUN
# =============================
if __name__ == "__main__":
    url = input("Enter any Jumia category/listing URL: ").strip()

    df = scrape_jumia_category(url)

    parsed = urlparse(url)
    path_name = os.path.basename(parsed.path) or "jumia_products"
    file_path = generate_unique_filename(path_name)

    df.to_csv(file_path, index=False)
    print(f"\n✅ Scraping complete! {len(df)} products saved to:\n📁 {file_path}")
