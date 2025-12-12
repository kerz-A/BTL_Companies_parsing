import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re, time
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl
import pandas as pd
from pathlib import Path

LIST_URL = "https://marketing-tech.ru/company_tags/btl/"
OUT_FILE = Path("data\raw\marketingtech_top20.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ---- session with retries
def make_session():
    s = requests.Session()
    retries = Retry(total=3, connect=3, read=3, backoff_factor=1.2,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

SESSION = make_session()

def fetch_html(url, timeout=25):
    try:
        resp = SESSION.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"[HTTP] {url} -> {e}")
        return ""

def text_or_none(el):
    return el.get_text(strip=True) if el else ""

def normalize_revenue(text):
    if not text: return ""
    s = text.lower().replace(" ", "")
    nums = re.findall(r"[\d.,]+", s)
    if "млрд" in s and nums:
        return int(float(nums[0].replace(",", ".")) * 1_000_000_000)
    if "млн" in s and nums:
        return int(float(nums[0].replace(",", ".")) * 1_000_000)
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else ""

def clean_site_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.netloc.endswith("marketing-tech.ru"):
        return ""
    block_params = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content",
                    "yclid","gclid","fbclid","utm_referrer","ref"}
    cleaned_qs = [(k,v) for k,v in parse_qsl(parsed.query) if k.lower() not in block_params]
    query = "&".join([f"{k}={v}" for k,v in cleaned_qs])
    cleaned = parsed._replace(query=query, fragment="")
    return urlunparse(cleaned)

def parse_int_value(text: str):
    if not text: return ""
    m = re.search(r"\d+", text.replace(" ", ""))
    return int(m.group(0)) if m else ""

def parse_company_card(card_url, throttle_sec=0.8):
    html = fetch_html(card_url)
    if not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")

    # --- Name: prefer h1 a, else strip star/age medal from h1 ---
    name = ""
    h1 = soup.select_one("header.company-header h1")
    if h1:
        h1_link = h1.select_one("a")
        if h1_link:
            name = text_or_none(h1_link)
        else:
            for bad in h1.select(".company-star, .company-age-medal"):
                bad.decompose()
            name = text_or_none(h1)

    # Сайт
    site_el = soup.select_one(".company-basics__table a.company-website-button")
    raw_site = site_el.get("href") if site_el else ""
    site = clean_site_url(raw_site)

    # Выручка
    revenue_text = text_or_none(soup.select_one(".company-flow div"))
    revenue = normalize_revenue(revenue_text)

    # Город, основана, штат
    city, founded, staff = "", "", ""
    for row in soup.select(".basic-information-table__column_about .table-row"):
        left = text_or_none(row.select_one(".table-row__col_1"))
        right = text_or_none(row.select_one(".table-row__col_2"))
        if "Город" in left: city = right
        elif "Основана" in left: founded = right
        elif "Штат" in left: staff = parse_int_value(right)

    specials = [text_or_none(a) for a in soup.select(".basic-information-table__column_specials a")]
    services = [text_or_none(a) for a in soup.select(".basic-information-table__column_services a")]
    tags = [text_or_none(a) for a in soup.select("figure.company-tags a.btn")]

    phone_el = soup.select_one(".company-basics__table a.full")
    phone = text_or_none(phone_el)

    address = ""
    for row in soup.select(".company-basics__table .table-row"):
        th = text_or_none(row.select_one(".th"))
        td = text_or_none(row.select_one(".td"))
        if "Адрес" in th:
            address = td

    description = text_or_none(soup.select_one(".about-company p"))

    time.sleep(throttle_sec)
    return {
        "inn": "",
        "name": name,
        "revenue_year": "",
        "revenue": revenue,
        "segment_tag": ";".join(tags) if tags else "BTL",
        "source": "marketingtech",
        "rating_ref": card_url,
        "okved_main": "",
        "employees": staff,
        "site": site,
        "description": description,
        "region": city,
        "contacts": phone,
        "founded": founded,
        "specializations": ";".join(specials),
        "services": ";".join(services),
        "address": address
    }

def extract_top20_links(list_html):
    soup = BeautifulSoup(list_html, "html.parser")
    table = soup.select_one("div.table-wrapper table")
    if not table:
        print("[ERROR] TOP table not found")
        return []
    links = []
    rows = table.find_all("tr")
    for tr in rows[1:]:
        a = tr.select_one("td:nth-of-type(2) a")
        if a and a.get("href"):
            links.append(urljoin(LIST_URL, a.get("href")))
        if len(links) == 20:
            break
    return links

def main():
    print(f"Parsing list: {LIST_URL}")
    list_html = fetch_html(LIST_URL)
    if not list_html:
        print("[ERROR] empty list HTML")
        return
    top20_links = extract_top20_links(list_html)
    print(f"Found {len(top20_links)} company links")

    records = []
    for i, link in enumerate(top20_links, 1):
        print(f"[CARD {i}/{len(top20_links)}] {link}")
        data = parse_company_card(link)
        if data.get("name"):
            records.append(data)

    # Фильтр по выручке ≥ 200 млн ₽
    filtered = [r for r in records if isinstance(r["revenue"], int) and r["revenue"] >= 200_000_000]

    df = pd.DataFrame(filtered)
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")
    print(f"Saved {len(df)} rows to {OUT_FILE}")

if __name__ == "__main__":
    main()
