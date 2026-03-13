#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 (加强版)
- 基于 95ge.py，修复所有站点抓取不完整问题
- 时间使用中国时间 UTC+8
- 所有域名保持完整路径
"""

import re, json, time, hashlib, logging, os
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CST = timezone(timedelta(hours=8))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|"
    r"proton|pm|email|out1ok|live|msn)\.[a-z]{2,}\b",
    re.IGNORECASE)

STATUS_BAD = {"异常","不可用","失效","已失效","暂无可用","unavailable","invalid","error","失效账号","暂无"}

def uid(email):
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]

def bad(status):
    if not status: return False
    s = status.lower().strip()
    return any(k in s for k in STATUS_BAD)

def now_cst():
    return datetime.now(CST).isoformat()

def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
        {"source":"Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
    return driver

def dedup(lst):
    seen, out = set(), []
    for r in lst:
        e = r.get("email","").lower().strip()
        if e and e not in seen:
            seen.add(e); out.append(r)
    return out

def parse_text(text):
    results, seen = [], set()
    INLINE = re.compile(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
        r"[\s\t]*(?:密码|password|pwd)?[\s:：/|｜,，\t ]*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})", re.IGNORECASE)
    CTX_PWD = re.compile(
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)
    for m in INLINE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if (e,p) not in seen and len(p)>=5:
            seen.add((e,p))
            results.append({"email":e,"password":p,"status":"正常","checked_at":""})
    lines = text.splitlines()
    for i, line in enumerate(lines):
        emails = EMAIL_RE.findall(line)
        if not emails: continue
        ctx = "\n".join(lines[max(0,i-2):i+5])
        m = CTX_PWD.search(ctx)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        if m:
            for e in emails:
                k = (e.lower(), m.group(1).strip())
                if k not in seen and len(k[1])>=5:
                    seen.add(k)
                    results.append({"email":k[0],"password":k[1],
                                    "status":"正常","checked_at":mt.group(1) if mt else ""})
    return results

JS_INPUTS = """
var out=[];
document.querySelectorAll('input').forEach(function(inp){
    var v=inp.value||'';
    if(v&&v.length>=5&&!v.includes('@')){
        var p=inp.closest('[class]')||inp.parentElement;
        var txt=p?p.innerText:'';
        var em=txt.match(/[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i);
        if(em) out.push({email:em[0],pwd:v,txt:txt});
    }
});
return out;
"""

def from_inputs(driver):
    try:
        data = driver.execute_script(JS_INPUTS)
        results, seen = [], set()
        for d in (data or []):
            e = d.get("email","").lower()
            p = d.get("pwd","")
            if e and p and "@" in e and e not in seen and len(p)>=5:
                seen.add(e)
                txt = d.get("txt","")
                mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", txt)
                results.append({"email":e,"password":p,"status":"正常",
                                "checked_at":mt.group(1) if mt else ""})
        return results
    except Exception:
        return []

def scroll(driver, n=8):
    for _ in range(n):
        driver.execute_script("window.scrollBy(0,700);")
        time.sleep(0.6)

def generic_parse(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","li","article","section","tr"], recursive=True):
        text = card.get_text(" ", strip=True)
        if len(text)<15: continue
        me = EMAIL_RE.search(text)
        if not me: continue
        mp = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        if not mp:
            after = text[me.end():]
            mp2 = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
            if not mp2: continue
            pwd = mp2.group(1)
        else:
            pwd = mp.group(1)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        ms = re.search(r"(正常|可用|Normal|异常|不可用)", text, re.I)
        status = ms.group(1) if ms else "正常"
        if bad(status): continue
        results.append({"email":me.group().lower(),"password":pwd,
                        "status":"正常","checked_at":mt.group(1) if mt else ""})
    return dedup(results)

# ──────────────────────────────────────────
# dongyubin GitHub API
# ──────────────────────────────────────────
def crawl_dongyubin_api():
    results = []
    urls = [
        "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/main/apple_share_ids.json",
        "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/main/ids.json",
        "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/master/apple_share_ids.json",
        "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/master/ids.json",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200: continue
            data = r.json()
            items = data if isinstance(data, list) else data.get("accounts", data.get("ids", []))
            for item in (items or []):
                if not isinstance(item, dict): continue
                email = (item.get("email") or item.get("account") or item.get("id") or "").lower()
                pwd = str(item.get("password") or item.get("pwd") or item.get("pass") or "")
                status = item.get("status","正常")
                if email and pwd and "@" in email and len(pwd)>=4 and not bad(status):
                    results.append({"email":email,"password":pwd,"status":"正常",
                                    "checked_at":item.get("checked_at","")})
            if results:
                logger.info(f"  dongyubin API ok: {len(results)} 条")
                break
        except Exception as e:
            logger.warning(f"  dongyubin {url}: {e}")
    return dedup(results)

def crawl_dongyubin_page(driver):
    for url in ["https://dongyubin.github.io/","https://dongyubin.github.io/appleid"]:
        try:
            driver.get(url); time.sleep(4); scroll(driver)
            soup = BeautifulSoup(driver.page_source,"html.parser")
            r = [p for p in parse_text(soup.get_text("\n")) if not bad(p.get("status",""))]
            if r: return dedup(r)
        except Exception: pass
    return []

# ──────────────────────────────────────────
# shadowsockshelp.github.io
# ──────────────────────────────────────────
def crawl_shadowsockshelp(driver):
    for url in [
        "https://shadowsockshelp.github.io/ios/apple-id-share.html",
        "https://shadowsockshelp.github.io/Shadowsocks/apple-id-share.html",
        "https://shadowsockshelp.github.io/",
    ]:
        try:
            driver.get(url); time.sleep(4); scroll(driver)
            soup = BeautifulSoup(driver.page_source,"html.parser")
            results = []
            for table in soup.find_all("table"):
                rows = table.find_all("tr")
                if not rows: continue
                headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
                ec = next((i for i,h in enumerate(headers) if "email" in h or "账" in h or "@" in h), None)
                pc = next((i for i,h in enumerate(headers) if "pass" in h or "密" in h or "pwd" in h), None)
                if ec is None or pc is None: continue
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cells) > max(ec,pc):
                        e,p = cells[ec].lower(), cells[pc]
                        if "@" in e and len(p)>=5:
                            results.append({"email":e,"password":p,"status":"正常","checked_at":""})
            if not results:
                results = [p for p in parse_text(soup.get_text("\n")) if not bad(p.get("status",""))]
            if results: return dedup(results)
        except Exception as e:
            logger.warning(f"  shadowsockshelp {url}: {e}")
    return []

# ──────────────────────────────────────────
# appledi.github.io
# ──────────────────────────────────────────
def crawl_appledi_github(driver):
    for url in ["https://appledi.github.io/","https://appledi.github.io/index.html"]:
        try:
            driver.get(url); time.sleep(4); scroll(driver)
            r = from_inputs(driver)
            if not r: r = generic_parse(driver)
            if r: return dedup(r)
        except Exception as e:
            logger.warning(f"  appledi {url}: {e}")
    return []

# ──────────────────────────────────────────
# ccbaohe.com/appleID
# ──────────────────────────────────────────
def crawl_ccbaohe(driver):
    driver.get("https://ccbaohe.com/appleID")
    time.sleep(5); scroll(driver)
    r = from_inputs(driver)
    if not r:
        soup = BeautifulSoup(driver.page_source,"html.parser")
        r = [p for p in parse_text(soup.get_text("\n")) if not bad(p.get("status",""))]
    return dedup(r)

# ──────────────────────────────────────────
# shadowrocket.best/
# ──────────────────────────────────────────
def crawl_shadowrocket_best(driver):
    driver.get("https://shadowrocket.best/")
    time.sleep(5)
    for _ in range(12):
        driver.execute_script("window.scrollBy(0,800);"); time.sleep(0.6)
    driver.execute_script("window.scrollTo(0,0)"); time.sleep(1)

    r = from_inputs(driver)
    seen = {x["email"] for x in r}

    soup = BeautifulSoup(driver.page_source,"html.parser")
    for card in soup.find_all(["div","li"], recursive=True):
        if len(list(card.children))<2: continue
        text = card.get_text(" ",strip=True)
        if len(text)<15: continue
        me = EMAIL_RE.search(text)
        if not me or me.group().lower() in seen: continue
        mp = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        if not mp:
            after = text[me.end():]
            mp2 = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
            if not mp2: continue
            pwd = mp2.group(1)
        else:
            pwd = mp.group(1)
        mt = re.search(r"更[新新]?[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        if not mt: mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        e = me.group().lower(); seen.add(e)
        r.append({"email":e,"password":pwd,"status":"正常",
                  "checked_at":mt.group(1) if mt else ""})
    return dedup(r)

# ──────────────────────────────────────────
# free.iosapp.icu/
# ──────────────────────────────────────────
def crawl_free_iosapp_icu(driver):
    driver.get("https://free.iosapp.icu/")
    time.sleep(5); scroll(driver)
    r = from_inputs(driver)
    seen = {x["email"] for x in r}
    soup = BeautifulSoup(driver.page_source,"html.parser")
    for card in soup.find_all(["div","section"], recursive=True):
        text = card.get_text(" ",strip=True)
        if len(text)<20: continue
        me = re.search(r"账[号号][:：\s]*(" + EMAIL_RE.pattern + r")", text, re.I)
        mp = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        ms = re.search(r"状[态態][:：\s]*(\S+)", text)
        mt = re.search(r"检查时间[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        if me and mp:
            e = me.group(1).lower()
            if e in seen: continue
            status = ms.group(1) if ms else "正常"
            if bad(status): continue
            seen.add(e)
            r.append({"email":e,"password":mp.group(1),"status":status,
                      "checked_at":mt.group(1) if mt else ""})
    return dedup(r)

# ──────────────────────────────────────────
# idfree.top/
# ──────────────────────────────────────────
def crawl_idfree_top(driver):
    driver.get("https://idfree.top/")
    time.sleep(4)
    for sel in ["//button[contains(.,'我已阅读')]","//button[contains(.,'继续查看')]",
                "//button[contains(.,'查看账号')]","//a[contains(.,'继续')]"]:
        try:
            btn = WebDriverWait(driver,6).until(EC.element_to_be_clickable((By.XPATH,sel)))
            driver.execute_script("arguments[0].click();",btn); time.sleep(3); break
        except Exception: pass
    scroll(driver)
    r = from_inputs(driver)
    if r: return dedup(r)
    emails = EMAIL_RE.findall(driver.page_source)
    inputs = driver.find_elements(By.CSS_SELECTOR,"input")
    pwds = [v for v in [driver.execute_script("return arguments[0].value;",i) for i in inputs]
            if v and len(v)>=5 and "@" not in v]
    soup = BeautifulSoup(driver.page_source,"html.parser")
    page_text = soup.get_text("\n")
    seen, out = set(), []
    for i,email in enumerate(emails):
        e = email.lower()
        if e in seen: continue
        pwd = pwds[i] if i<len(pwds) else (pwds[0] if pwds else "")
        if not pwd: continue
        idx = page_text.find(email)
        ctx = page_text[max(0,idx-50):idx+200] if idx>=0 else ""
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        ms = re.search(r"(正常|异常|可用)", ctx)
        status = ms.group(1) if ms else "正常"
        if bad(status): continue
        seen.add(e)
        out.append({"email":e,"password":pwd,"status":"正常","checked_at":mt.group(1) if mt else ""})
    return out

# ──────────────────────────────────────────
# id.btvda.top/
# ──────────────────────────────────────────
def crawl_id_btvda_top(driver):
    driver.get("https://id.btvda.top/")
    time.sleep(5); scroll(driver)
    r = from_inputs(driver)
    if not r: r = generic_parse(driver)
    if not r:
        soup = BeautifulSoup(driver.page_source,"html.parser")
        r = [p for p in parse_text(soup.get_text("\n")) if not bad(p.get("status",""))]
    return dedup(r)

# ──────────────────────────────────────────
# idshare001.me/goso.html
# ──────────────────────────────────────────
def crawl_idshare001(driver):
    driver.get("https://idshare001.me/goso.html")
    time.sleep(5)
    results = []
    try:
        data = driver.execute_script("""
            var out=[];
            document.querySelectorAll('[data-account],[data-email],[data-id],[data-username]').forEach(function(el){
                var email=el.getAttribute('data-account')||el.getAttribute('data-email')||
                          el.getAttribute('data-id')||el.getAttribute('data-username')||'';
                var pwd=el.getAttribute('data-password')||el.getAttribute('data-pwd')||'';
                if(email&&email.includes('@')) out.push({email:email,pwd:pwd});
            });
            return out;
        """)
        for d in (data or []):
            if d.get("email") and d.get("pwd") and len(d["pwd"])>=5:
                results.append({"email":d["email"].lower(),"password":d["pwd"],"status":"正常","checked_at":""})
    except Exception: pass
    if not results:
        scroll(driver)
        r2 = from_inputs(driver)
        if not r2: r2 = generic_parse(driver)
        results = r2
    return dedup(results)

# ──────────────────────────────────────────
# app.iosr.cn/tools/apple-shared-id
# ──────────────────────────────────────────
def crawl_app_iosr_cn(driver):
    driver.get("https://app.iosr.cn/tools/apple-shared-id")
    time.sleep(6)
    try:
        driver.find_element(By.XPATH,"//button[contains(.,'刷新')]").click(); time.sleep(3)
    except Exception: pass
    scroll(driver)
    r = from_inputs(driver)
    if r: return dedup(r)
    soup = BeautifulSoup(driver.page_source,"html.parser")
    results = []
    for card in soup.find_all(["div","li","article"], recursive=True):
        text = card.get_text(" ",strip=True)
        if len(text)<15: continue
        me = EMAIL_RE.search(text)
        if not me: continue
        mp = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        if not mp:
            after = text[me.end():]
            mp2 = re.search(r"\b([A-Za-z0-9]{8,24})\b", after)
            if not mp2: continue
            pwd = mp2.group(1)
        else:
            pwd = mp.group(1)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        ms = re.search(r"(正常|正常使用|可用|Normal)", text, re.I)
        status = ms.group(1) if ms else "正常"
        if bad(status): continue
        results.append({"email":me.group().lower(),"password":pwd,"status":"正常",
                        "checked_at":mt.group(1) if mt else ""})
    return dedup(results)

# ──────────────────────────────────────────
# id.bocchi2b.top/
# ──────────────────────────────────────────
def crawl_bocchi2b(driver):
    driver.get("https://id.bocchi2b.top/")
    time.sleep(3)
    for sel in ["//button[text()='Ok']","//button[text()='OK']","//button[contains(@class,'ok')]",
                "//div[contains(@class,'modal')]//button"]:
        try:
            btn = WebDriverWait(driver,4).until(EC.element_to_be_clickable((By.XPATH,sel)))
            driver.execute_script("arguments[0].click();",btn); time.sleep(1); break
        except Exception: pass
    scroll(driver)
    r = from_inputs(driver)
    if not r: r = generic_parse(driver)
    return dedup(r)

# ──────────────────────────────────────────
# 139.196.183.52/share/DZhBvnglEU
# ──────────────────────────────────────────
def crawl_ip_share(driver):
    driver.get("http://139.196.183.52/share/DZhBvnglEU")
    time.sleep(5); scroll(driver)
    r = from_inputs(driver)
    if not r: r = generic_parse(driver)
    return dedup(r)

# ──────────────────────────────────────────
# nodeba.com/
# ──────────────────────────────────────────
def crawl_nodeba(driver):
    driver.get("https://nodeba.com/")
    time.sleep(4)
    results = []
    try:
        links = driver.find_elements(By.CSS_SELECTOR,
            "article a,h2 a,h3 a,.post-title a,.entry-title a,.post a")
        article_url = None
        for link in links:
            href = link.get_attribute("href") or ""
            txt  = link.text or ""
            if "nodeba.com" in href and href != "https://nodeba.com/" and \
               any(kw in txt for kw in ["Apple","apple","ID","账号","共享","苹果"]):
                article_url = href; break
        if not article_url and links:
            article_url = links[0].get_attribute("href")
        if article_url:
            driver.get(article_url); time.sleep(4)
            soup = BeautifulSoup(driver.page_source,"html.parser")
            results = [p for p in parse_text(soup.get_text("\n")) if not bad(p.get("status",""))]
    except Exception as e:
        logger.error(f"  nodeba: {e}")
    return dedup(results)

# ──────────────────────────────────────────
# tkbaohe.com/Shadowrocket/
# ──────────────────────────────────────────
def crawl_tkbaohe(driver):
    driver.get("https://tkbaohe.com/Shadowrocket/")
    time.sleep(5); scroll(driver)
    r = from_inputs(driver)
    if r: return dedup(r)
    soup = BeautifulSoup(driver.page_source,"html.parser")
    r2 = [p for p in parse_text(soup.get_text("\n")) if not bad(p.get("status",""))]
    return dedup(r2)

# ──────────────────────────────────────────
# ios.aneeo.com/
# ──────────────────────────────────────────
def crawl_ios_aneeo(driver):
    driver.get("https://ios.aneeo.com/")
    time.sleep(5)
    for sel in ["//button[contains(.,'知道了')]","//button[contains(.,'我知道了')]",
                "//button[contains(.,'确定')]","//button[contains(.,'关闭')]"]:
        try:
            btn = WebDriverWait(driver,3).until(EC.element_to_be_clickable((By.XPATH,sel)))
            driver.execute_script("arguments[0].click();",btn); time.sleep(1); break
        except Exception: pass
    scroll(driver)
    r = from_inputs(driver)
    if not r: r = generic_parse(driver)
    return dedup(r)

# ──────────────────────────────────────────
# clashid.com.cn/
# ──────────────────────────────────────────
def crawl_clashid(driver):
    for url in ["https://clashid.com.cn/","http://clashid.com.cn/"]:
        try:
            driver.get(url); time.sleep(5); scroll(driver)
            r = from_inputs(driver)
            if not r:
                soup = BeautifulSoup(driver.page_source,"html.parser")
                r = [p for p in parse_text(soup.get_text("\n")) if not bad(p.get("status",""))]
            if r: return dedup(r)
        except Exception as e:
            logger.warning(f"  clashid {url}: {e}")
    return []


SITES = [
    {"name":"shadowsockshelp.github.io", "fn":crawl_shadowsockshelp},
    {"name":"appledi.github.io",          "fn":crawl_appledi_github},
    {"name":"ccbaohe.com/appleID",        "fn":crawl_ccbaohe},
    {"name":"shadowrocket.best",          "fn":crawl_shadowrocket_best},
    {"name":"free.iosapp.icu",            "fn":crawl_free_iosapp_icu},
    {"name":"idfree.top",                 "fn":crawl_idfree_top},
    {"name":"id.btvda.top",              "fn":crawl_id_btvda_top},
    {"name":"idshare001.me",              "fn":crawl_idshare001},
    {"name":"app.iosr.cn",               "fn":crawl_app_iosr_cn},
    {"name":"id.bocchi2b.top",           "fn":crawl_bocchi2b},
    {"name":"139.196.183.52",            "fn":crawl_ip_share},
    {"name":"nodeba.com",                "fn":crawl_nodeba},
    {"name":"tkbaohe.com",              "fn":crawl_tkbaohe},
    {"name":"ios.aneeo.com",             "fn":crawl_ios_aneeo},
    {"name":"clashid.com.cn",            "fn":crawl_clashid},
]

def crawl_all():
    seen, source_stats = {}, {}

    # dongyubin API
    logger.info("▶ 抓取 dongyubin API...")
    try:
        pairs = crawl_dongyubin_api()
        nc = 0
        for p in pairs:
            e = p.get("email","").strip().lower()
            pw = p.get("password","").strip()
            if not e or not pw or "@" not in e or len(pw)<4: continue
            if len(set(pw))<2: continue
            if e not in seen:
                seen[e] = {"id":uid(e),"email":e,"password":pw,"status":p.get("status","正常"),
                           "checked_at":p.get("checked_at",""),"source":"dongyubin.github(API)",
                           "updated_at":now_cst()}
                nc += 1
        source_stats["dongyubin.github(API)"] = nc
        logger.info(f"  → 新增 {nc} 条")
    except Exception as e:
        logger.error(f"  dongyubin API: {e}")
        source_stats["dongyubin.github(API)"] = 0

    logger.info("启动浏览器...")
    driver = make_driver()
    try:
        for site in SITES:
            logger.info(f"▶ 抓取: {site['name']}")
            try:
                pairs = site["fn"](driver)
            except Exception as e:
                logger.error(f"  {site['name']}: {e}")
                pairs = []
            nc = 0
            for p in pairs:
                e = p.get("email","").strip().lower()
                pw = p.get("password","").strip()
                if not e or not pw or "@" not in e or len(pw)<4: continue
                if len(set(pw))<2: continue
                if e not in seen:
                    seen[e] = {"id":uid(e),"email":e,"password":pw,"status":p.get("status","正常"),
                               "checked_at":p.get("checked_at",""),"source":site["name"],
                               "updated_at":now_cst()}
                    nc += 1
            source_stats[site["name"]] = nc
            logger.info(f"  → 新增 {nc} 条（共 {len(seen)} 条）")
            time.sleep(2)

        # dongyubin 页面补充
        if source_stats.get("dongyubin.github(API)",0) == 0:
            logger.info("▶ dongyubin 页面补充...")
            try:
                pairs = crawl_dongyubin_page(driver)
                nc = 0
                for p in pairs:
                    e = p.get("email","").strip().lower()
                    pw = p.get("password","").strip()
                    if not e or not pw or "@" not in e or len(pw)<4: continue
                    if e not in seen:
                        seen[e] = {"id":uid(e),"email":e,"password":pw,"status":"正常",
                                   "checked_at":"","source":"dongyubin.github(page)",
                                   "updated_at":now_cst()}
                        nc += 1
                source_stats["dongyubin.github(API)"] = nc
                logger.info(f"  → 补充 {nc} 条")
            except Exception as e:
                logger.error(f"  dongyubin page: {e}")
    finally:
        driver.quit()
        logger.info("浏览器已关闭")

    accounts = sorted(seen.values(), key=lambda a: a.get("checked_at","") or a.get("updated_at",""), reverse=True)
    return {
        "generated_at": datetime.now(CST).isoformat(),
        "total": len(accounts),
        "source_stats": source_stats,
        "accounts": accounts,
    }

if __name__ == "__main__":
    output_path = os.environ.get("OUTPUT_FILE","apple_ids.json")
    result = crawl_all()
    with open(output_path,"w",encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 完成！共输出 {result['total']} 条账号 → {output_path}")
