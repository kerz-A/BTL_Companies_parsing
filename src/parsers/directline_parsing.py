import requests, certifi, urllib3, re, time, html as ihtml
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import pandas as pd
from pathlib import Path

URL = "https://www.directline.pro/blog/pr-agentstva/"
BASE = "https://www.directline.pro"
OUT_FILE = Path("data\raw\directline_pr_agencies.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_html(url, allow_redirects=True):
    r = requests.get(url, timeout=20, verify=certifi.where(), allow_redirects=allow_redirects)
    r.raise_for_status()
    return r.text, r.url

def text(el): return el.get_text(" ", strip=True) if el else ""
def clean(s): return ihtml.unescape(s).strip()

def normalize_site(url):
    """
    Оставляем только scheme://netloc/ (без query/fragment/utm/anchors).
    """
    p = urlparse(url)
    if not p.scheme or not p.netloc:
        return ""
    # Если нужен корень — принудительно добавим завершающий слэш
    return f"{p.scheme}://{p.netloc}/"

def extract_attrs(block):
    """
    Извлекаем значения по меткам, устойчиво к разметке и стилевым span.
    """
    raw = block.get_text("\n", strip=True)
    def after(label):
        m = re.search(rf"{label}\s*[\n\r]+([^\n\r]+)", raw, re.I)
        return clean(m.group(1)) if m else ""
    city = after("Город:")
    founded = after("Год основания компании:")
    return city, founded

def resolve_site(href):
    """
    Определяем реальный сайт:
    - Если внешняя ссылка не на directline/dlrecommend — нормализуем и возвращаем.
    - Если /recommend/... — идём внутрь, следуем редиректам.
    - Если попали на dlrecommend.ru — берём rurl из query, нормализуем.
    - Если попали на /lander/... с rurl — берём rurl, нормализуем.
    - Иначе пытаемся найти внешние ссылки внутри HTML.
    """
    # Абсолютный href
    if href.startswith("/"):
        href = urljoin(BASE, href)

    # Если сразу внешний, и это не домены-трекеры
    parsed = urlparse(href)
    host = parsed.netloc.lower()
    if host and ("directline.pro" not in host) and ("dlrecommend.ru" not in host):
        return normalize_site(href)

    # Переход по recommend/lander
    html, final_url = fetch_html(href, allow_redirects=True)
    pf = urlparse(final_url)
    host_f = pf.netloc.lower()

    # dlrecommend: URL содержит rurl в query
    if "dlrecommend.ru" in host_f:
        q = parse_qs(pf.query)
        rurl = (q.get("rurl") or [None])[0]
        if rurl and rurl.startswith("http"):
            return normalize_site(rurl)

    # directline lander: может быть rurl в query
    if "directline.pro" in host_f:
        q = parse_qs(pf.query)
        rurl = (q.get("rurl") or [None])[0]
        if rurl and rurl.startswith("http"):
            return normalize_site(rurl)

    # Иначе пробуем найти rurl внутри HTML
    m = re.search(r'rurl=([^\s"&]+)', html)
    if m:
        candidate = clean(m.group(1)).replace("&amp;", "&")
        if candidate.startswith("http"):
            return normalize_site(candidate)

    # Ищем явные внешние ссылки внутри страницы
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select('a[href^="http"]'):
        ah = a.get("href", "")
        ph = urlparse(ah)
        h = ph.netloc.lower()
        if h and ("directline.pro" not in h) and ("dlrecommend.ru" not in h):
            return normalize_site(ah)

    # Фолбэк: если финальный URL внешний — нормализуем его, иначе пусто
    if host_f and ("directline.pro" not in host_f) and ("dlrecommend.ru" not in host_f):
        return normalize_site(final_url)

    return ""

def parse():
    html, _ = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")
    records = []

    for item in soup.select("div.blog-table-item"):
        # Название
        name = clean(text(item.select_one("div.blog-table-item__title a")))

        # Атрибуты
        region, founded = "", ""
        for block in item.select("div.blog-table-item__text"):
            rg, fd = extract_attrs(block)
            region = rg or region
            founded = fd or founded

        # Сайт
        site = ""
        btn = item.select_one("a.blog-table-item__button[href]")
        if btn:
            site = resolve_site(btn["href"].strip())

        # Описание / теги
        lis = item.select("div.blog-table-item__list li")
        tags = [clean(text(li)) for li in lis]
        desc = " ".join(tags)

        # Картинка
        img = item.select_one("div.blog-table-item__logo img")
        img_src = clean(img.get("data-lazy-src") or img.get("src","")) if img else ""
        img_alt = clean(img.get("alt","")) if img else ""

        # rating_ref — исходная ссылка из кнопки (как в прошлом парсере)
        rating_ref = ""
        if btn:
            href = btn["href"].strip()
            rating_ref = urljoin(BASE, href) if href.startswith("/") else href

        records.append({
            "name": name,
            "region": region,
            "site": site,
            "contacts": "",
            "email": "",
            "address": "",
            "founded": founded,
            "segment_tag": ";".join(tags),
            "description": desc,
            "rating_ref": rating_ref,
            "source": "directline.pro",
            "img_src": img_src,
            "img_alt": img_alt
        })
        time.sleep(0.2)

    df = pd.DataFrame(records).drop_duplicates(subset=["name","site"])
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")
    print("Saved", len(df), "rows to", OUT_FILE)

if __name__ == "__main__":
    parse()
