import requests, certifi, urllib3, html as ihtml
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

URL = "https://pavezlo.ru/rejtingi/rejting-marketingovyh-agentstv-2025-70-luchshih-agentstv-marketinga/"
OUT_FILE = Path("data/pavezlo_marketing_agencies.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_html(url):
    r = requests.get(url, timeout=20, verify=certifi.where())
    r.raise_for_status()
    return r.text

def clean(s): return ihtml.unescape(s).strip()

def parse():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")
    records = []

    # проходим по всем секциям рейтинга
    for section in soup.select("h3.wp-block-heading"):
        heading = clean(section.get_text())
        region = ""
        if "СПБ" in heading.upper():
            region = "Санкт-Петербург"
        elif "МОСКВ" in heading.upper():
            region = "Москва"

        # следующий список <ol>
        ol = section.find_next("ol")
        if not ol:
            continue

        for li in ol.select("li a[href]"):
            name = clean(li.get_text())
            site = li["href"].strip()
            records.append({
                "name": name,
                "region": region,
                "site": site,
                "contacts": "",
                "email": "",
                "address": "",
                "founded": "",
                "segment_tag": "",
                "description": "",
                "rating_ref": site,
                "source": "pavezlo.ru",
                "img_src": "",
                "img_alt": ""
            })

    df = pd.DataFrame(records).drop_duplicates(subset=["name","site"])
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")
    print("Saved", len(df), "rows to", OUT_FILE)

if __name__ == "__main__":
    parse()
