import requests, certifi, urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re, time
from urllib.parse import urljoin
import pandas as pd
from pathlib import Path

LIST_URL = "https://www.alladvertising.ru/top/btl/"
BASE_ORIGIN = "https://www.alladvertising.ru"
OUT_FILE = Path("data\raw\alladvertising_top20.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1.2,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent":"Mozilla/5.0"})
    return s

SESSION = make_session()

def text(el): return el.get_text(strip=True) if el else ""

def fetch_html(url):
    try:
        r = SESSION.get(url, timeout=20, verify=certifi.where())
        r.raise_for_status(); return r.text
    except requests.exceptions.SSLError:
        try:
            r = SESSION.get(url.replace("https://","http://"), timeout=20)
            r.raise_for_status(); return r.text
        except:
            r = SESSION.get(url, timeout=20, verify=False)
            r.raise_for_status(); return r.text
    except: return ""

# --- список ТОП-20 ---
def extract_top20_links(html):
    soup = BeautifulSoup(html,"html.parser")
    links, preview = [], []
    for li in soup.select("div#s20 li.rate20"):
        a = li.select_one("h2 a[href]")
        if not a: continue
        name = text(a)
        link = urljoin(BASE_ORIGIN, a["href"])
        h2_text = li.select_one("h2").get_text(" ",strip=True)
        city = h2_text.replace(name,"").replace("/","").strip(" ,")
        desc = text(li.select_one("small"))
        img = li.select_one("img")
        img_src = urljoin(BASE_ORIGIN,img["src"]) if img else ""
        img_alt = img.get("alt","") if img else ""
        links.append(link)
        preview.append({"name":name,"region":city,"short_description":desc,
                        "img_src":img_src,"img_alt":img_alt})
    # спонсор
    sponsor = soup.select_one("div#sponsor div.company h2 a[href]")
    if sponsor:
        links.append(urljoin(BASE_ORIGIN,sponsor["href"]))
        preview.append({"name":text(sponsor),
                        "region":sponsor.find_parent("h2").get_text(" ",strip=True).replace(text(sponsor),"").replace("/","").strip(" ,"),
                        "short_description":text(sponsor.find_parent("h2").find_next("small")),
                        "img_src":urljoin(BASE_ORIGIN,soup.select_one("div#sponsor img")["src"]),
                        "img_alt":soup.select_one("div#sponsor img").get("alt","")})
    return links, preview

# --- карточка ---
def parse_card(url, preview):
    html = fetch_html(url)
    if not html: return {}
    soup = BeautifulSoup(html,"html.parser")
    name = text(soup.select_one("span.h1_700b")) or text(soup.select_one("h1"))
    city = text(soup.select_one("span.h1_300")).lstrip(", ") if soup.select_one("span.h1_300") else ""
    site = ""
    site_el = soup.select_one("div.sitem a[href^='http']")
    if site_el: site = site_el["href"]
    if not site:
        site_label = soup.find(string=re.compile("Сайт",re.I))
        if site_label:
            a = site_label.find_parent().find("a",href=True)
            if a: site = a["href"]
    phone = ""
    tel = soup.select_one('a[href^="tel:"]')
    if tel: phone = text(tel)
    if not phone:
        m = re.search(r"\+?\d[\d\s().-]+", soup.get_text(" ",strip=True))
        if m: phone = m.group(0)
    email = ""
    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", soup.get_text(" ",strip=True))
    if m: email = m.group(0)
    addr = ""
    addr_span = soup.select_one("span#toggle")
    if addr_span: addr = text(addr_span)
    else:
        addr_node = soup.find(string=re.compile("Адрес",re.I))
        if addr_node: addr = addr_node.parent.get_text(" ",strip=True)
    desc = text(soup.select_one("div.text span.preview")) or text(soup.select_one("div.review"))
    tags = [text(a) for a in soup.select("span.tagblock a.newtag")]
    founded = ""
    m = re.search(r"основан[оая]?.{0,20}?(\d{4})", soup.get_text(" ",strip=True), re.I)
    if m: founded = m.group(1)
    return {
        "name": name or preview.get("name",""),
        "region": city or preview.get("region",""),
        "site": site,
        "contacts": phone,
        "email": email,
        "address": addr,
        "founded": founded,
        "segment_tag": ";".join(tags),
        "description": desc or preview.get("short_description",""),
        "rating_ref": url,
        "source": "alladvertising",
        "img_src": preview.get("img_src",""),
        "img_alt": preview.get("img_alt","")
    }

def main():
    html = fetch_html(LIST_URL)
    links, previews = extract_top20_links(html)
    records = []
    for link, prev in zip(links, previews):
        print("Parsing:", link)
        data = parse_card(link, prev)
        if data: records.append(data)
        time.sleep(0.5)
    df = pd.DataFrame(records).drop_duplicates(subset=["name","site"])
    df.to_csv(OUT_FILE,index=False,encoding="utf-8")
    print("Saved",len(df),"rows to",OUT_FILE)

if __name__=="__main__":
    main()
