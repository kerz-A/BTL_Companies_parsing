import re
import time
import certifi
import urllib3
import requests
from pathlib import Path
from urllib.parse import urlparse, urljoin, quote
import pandas as pd
from bs4 import BeautifulSoup
from transformers import pipeline

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Files
INPUT_FILE = Path(r"ddata\interim\agencies_merged.csv")
OUTPUT_FILE = Path(r"data\interim\agencies_merged_with_inn_ogrn.csv")

# Local NER model (English)
NER_MODEL_PATH = r"src\utils\nermodel"
ner = pipeline("ner", model=NER_MODEL_PATH, grouped_entities=True)

# Regexes
RE_INN = re.compile(r"\b\d{10}\b|\b\d{12}\b")
RE_OGRN = re.compile(r"\b\d{13}\b")
RE_COMPANY = re.compile(r"(?:ООО|ОАО|ЗАО|ПАО|ИП)\s+[\"«]?[A-Za-zА-Яа-яЁё0-9\s\-\.\,]+[\"»]?", re.U)

RE_EMAIL = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
RE_PHONE = re.compile(
    r"\+7\s?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}"
    r"|\b8\s?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}"
)
RE_ADDRESS = re.compile(
    r"(?:Адрес|Юридический адрес|Фактический адрес)[^\n:]{0,10}[:\-–]\s?[^\n]{10,200}"
)
RE_REGION = re.compile(
    r"\bг\.\s?[А-ЯЁ][а-яё\-]+|\bСанкт[- ]?Петербург|\b[А-ЯЁ][а-яё]+ская область"
)
RE_REVENUE_YEAR = re.compile(
    r"(?:за\s)?(?:(?:\b20\d{2}\b)|(?:\b19\d{2}\b))(?=\s*г(?:\.|ода)?)"
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Firefox/121.0",
]

def build_session(ua_idx=0, verify=True, timeout=20):
    sess = requests.Session()
    retries = urllib3.util.retry.Retry(
        total=2, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": USER_AGENTS[ua_idx % len(USER_AGENTS)]})
    sess.timeout = timeout
    sess.verify = certifi.where() if verify else False
    return sess

def robust_get(url, timeout=20, stream=False):
    url = quote(url, safe=":/?&=%")
    for ua_idx in range(len(USER_AGENTS)):
        try:
            sess = build_session(ua_idx, verify=True, timeout=timeout)
            r = sess.get(url, timeout=timeout, stream=stream)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"[WARN] fetch fail UA#{ua_idx} {url}: {e}")
            time.sleep(1.0)
    return None

def extract_text(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(" ", strip=True)
    except Exception:
        return ""

def find_inn_ogrn_in_text(text):
    inns = RE_INN.findall(text or "")
    ogrns = RE_OGRN.findall(text or "")
    inn = inns[0] if inns else ""
    ogrn = ogrns[0] if ogrns else ""
    return inn, ogrn

def find_company_name(text):
    if not text:
        return ""
    m = RE_COMPANY.search(text)
    if m:
        return m.group(0).strip()
    # Long form
    m2 = re.search(
        r"Общество с ограниченной ответственностью\s+[\"«]?[A-Za-zА-Яа-яЁё0-9\s\-\.\,]+[\"»]?",
        text,
        re.U,
    )
    if m2:
        return m2.group(0).strip()
    return ""

def extract_contacts(text):
    txt = text or ""
    emails = RE_EMAIL.findall(txt)
    phones = RE_PHONE.findall(txt)
    address_m = RE_ADDRESS.search(txt)
    region_m = RE_REGION.search(txt)
    region = region_m.group(0).strip() if region_m else ""
    address = address_m.group(0).strip() if address_m else ""
    contacts = "; ".join(sorted(set(phones))) if phones else ""
    email = "; ".join(sorted(set(emails))) if emails else ""
    return region, address, contacts, email

def parse_revenue_year(text):
    txt = text or ""
    m = RE_REVENUE_YEAR.search(txt)
    if m:
        return m.group(0)
    return ""

def gather_pdf_links_from_dom(soup, base_root):
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(base_root, href)
        if href.lower().endswith(".pdf"):
            links.append(href)
    return list(set(links))

def parse_pdf_to_text(url):
    r = robust_get(url, timeout=45, stream=True)
    if not r:
        return ""
    content = b""
    # Stream up to 2MB to avoid huge downloads
    for chunk in r.iter_content(chunk_size=1024 * 64):
        content += chunk
        if len(content) > 2_000_000:
            break
    # Try simple decode (heuristic). For more reliable text, integrate pdfminer or PyMuPDF.
    try:
        return content.decode("latin-1", errors="ignore")
    except Exception:
        return ""

def ner_extract(text):
    # Use NER to supplement regex extraction; still confirm via regex to reduce noise
    try:
        entities = ner(text or "")
    except Exception as e:
            print(f"[WARN] NER failed: {e}")
            return "", "", ""
    inn, ogrn, company = "", "", ""
    for ent in entities:
        val = ent.get("word", "")
        # Merge subwords if needed
        val = val.replace("##", "")
        if not inn and re.fullmatch(r"\d{10}|\d{12}", val):
            inn = val
        elif not ogrn and re.fullmatch(r"\d{13}", val):
            ogrn = val
        elif not company and any(val.startswith(prefix) for prefix in ["ООО", "ОАО", "ЗАО", "ПАО", "ИП"]):
            company = val
    return inn, ogrn, company

def fetch_homepage_and_text(site):
    root = f"{urlparse(site).scheme}://{urlparse(site).netloc}/"
    r = robust_get(root, timeout=20)
    if not r:
        return "", "", None, root
    text = extract_text(r.text)
    soup = BeautifulSoup(r.text, "html.parser")
    return text, r.text, soup, root

def search_all_pdfs(root, soup):
    pdf_links = []
    # From DOM
    if soup:
        pdf_links.extend(gather_pdf_links_from_dom(soup, root))
    # From sitemap.xml
    sitemap_url = urljoin(root, "sitemap.xml")
    r2 = robust_get(sitemap_url, timeout=20)
    if r2:
        sm_text = r2.text
        pdfs = re.findall(r"https?://[^\s\"']+\.pdf", sm_text)
        pdf_links.extend(pdfs)
    return list(dict.fromkeys(pdf_links))  # unique, preserve order

def parse_fields_from_html(text, prefer_if_missing=True, current=None):
    """
    Extract fields from HTML text. If prefer_if_missing=True and current dict has
    empty fields, fill them; otherwise, keep existing values.
    """
    inn_h, ogrn_h = find_inn_ogrn_in_text(text)
    company_h = find_company_name(text)
    region_h, address_h, contacts_h, email_h = extract_contacts(text)
    rev_year_h = parse_revenue_year(text)

    result = {
        "inn": inn_h,
        "ogrn": ogrn_h,
        "full_name": company_h,
        "region": region_h,
        "address": address_h,
        "contacts": contacts_h,
        "email": email_h,
        "revenue_year": rev_year_h,
        "doc_url": "",
        "doc_type": "homepage",
    }
    if prefer_if_missing and current:
        for k, v in result.items():
            if not str(current.get(k, "")).strip() and v:
                current[k] = v
        return current
    return result

def parse_fields_from_pdf(url, current=None):
    pdf_text = parse_pdf_to_text(url)
    if not pdf_text:
        return current or {}
    inn_p, ogrn_p = find_inn_ogrn_in_text(pdf_text)
    company_p = find_company_name(pdf_text)
    # Supplement with NER (heuristic)
    inn_n, ogrn_n, company_n = ner_extract(pdf_text)
    inn_final = inn_p or inn_n
    ogrn_final = ogrn_p or ogrn_n
    company_final = company_p or company_n
    region_p, address_p, contacts_p, email_p = extract_contacts(pdf_text)
    rev_year_p = parse_revenue_year(pdf_text)

    parsed = {
        "inn": inn_final,
        "ogrn": ogrn_final,
        "full_name": company_final,
        "region": region_p,
        "address": address_p,
        "contacts": contacts_p,
        "email": email_p,
        "revenue_year": rev_year_p,
        "doc_url": url,
        "doc_type": "pdf",
    }
    if current:
        for k, v in parsed.items():
            if not str(current.get(k, "")).strip() and v:
                current[k] = v
        # Prefer doc_url/doc_type if we actually found any main fields
        if (parsed["inn"] or parsed["ogrn"] or parsed["full_name"]) and parsed["doc_url"]:
            current["doc_url"] = parsed["doc_url"]
            current["doc_type"] = parsed["doc_type"]
        return current
    return parsed

def process_site(row, col_order):
    site = str(row.get("site", "")).strip()
    if not site:
        return row

    # Build a mutable record of parsed fields, seeded with existing values (do not overwrite)
    current = {
        "inn": str(row.get("inn", "")).strip(),
        "ogrn": str(row.get("ogrn", "")).strip(),
        "full_name": str(row.get("full_name", "")).strip(),
        "region": str(row.get("region", "")).strip(),
        "address": str(row.get("address", "")).strip(),
        "contacts": str(row.get("contacts", "")).strip(),
        "email": str(row.get("email", "")).strip(),
        "revenue_year": str(row.get("revenue_year", "")).strip(),
        "revenue": str(row.get("revenue", "")).strip(),
        "doc_url": str(row.get("doc_url", "")).strip(),
        "doc_type": str(row.get("doc_type", "")).strip(),
    }

    print(f"[INFO] Обрабатываю сайт: {site}")

    # 1) Homepage HTML
    text, html_raw, soup, root = fetch_homepage_and_text(site)
    if text:
        current = parse_fields_from_html(text, prefer_if_missing=True, current=current)

    # 2) All PDFs from site (DOM + sitemap.xml)
    pdf_links = search_all_pdfs(root, soup)
    if pdf_links:
        print(f"   [INFO] Найдено PDF: {len(pdf_links)}")
    for link in pdf_links:
        current = parse_fields_from_pdf(link, current=current)

    # Final assembly: keep original metadata from source file
    # name, site, segment_tag, source come from input as-is
    current["name"] = row.get("name", "")
    current["site"] = site
    current["segment_tag"] = row.get("segment_tag", "")
    current["source"] = row.get("source", "")
    # revenue: leave as-is if provided; otherwise keep parsed or empty
    current["revenue"] = row.get("revenue", current["revenue"])

    # Order columns and set back to row
    for col in col_order:
        row[col] = current.get(col, row.get(col, ""))

    return row

def main():
    # Load input
    df = pd.read_csv(INPUT_FILE, encoding="utf-8")

    # Resume mode: if output exists, continue from it (preserve parsed progress)
    if OUTPUT_FILE.exists():
        prev = pd.read_csv(OUTPUT_FILE, encoding="utf-8")
        # Align columns and merge by index
        df = prev.reindex(columns=list(prev.columns)) \
                 .combine_first(df) \
                 .fillna("")
        print(f"[INFO] Продолжаем с существующего файла: {len(df)} строк.")

    # Ensure required columns exist
    required_cols = [
        "inn", "ogrn", "name", "full_name", "site",
        "region", "address", "contacts", "email",
        "revenue_year", "revenue", "segment_tag", "source",
        "doc_url", "doc_type",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    # Column order exactly as requested
    col_order = [
        "inn", "ogrn", "name", "full_name", "site",
        "region", "address", "contacts", "email",
        "revenue_year", "revenue", "segment_tag", "source",
        "doc_url", "doc_type",
    ]

    total = len(df)
    print(f"[INFO] Начинаем обработку {total} сайтов...")

    for idx, row in df.iterrows():
        site = str(row.get("site", "")).strip()
        if not site:
            continue

        # Skip already parsed rows if inn/ogrn/full_name present and contacts/email/address/region/revenue_year filled
        already = (
            (str(row.get("inn", "")).strip() or str(row.get("ogrn", "")).strip() or str(row.get("full_name", "")).strip())
            and (str(row.get("contacts", "")).strip() or str(row.get("email", "")).strip()
                 or str(row.get("address", "")).strip() or str(row.get("region", "")).strip())
        )
        if already:
            continue

        try:
            updated_row = process_site(row, col_order)
            for c in col_order:
                df.at[idx, c] = updated_row[c]
            # Save after each processed site
            df[col_order].to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
            print(f"   [SAVE] {OUTPUT_FILE} обновлён (строка {idx+1})")
        except Exception as e:
            print(f"[ERROR] Ошибка обработки {site}: {e}")
            # Save progress even on error
            df[col_order].to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    # Final save ensuring column order
    df[col_order].to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"[INFO] Готово! Сохранено {len(df)} строк в {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
