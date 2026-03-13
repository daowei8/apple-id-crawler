#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 v3
- 完整站点列表（16站点）
- GitHub API 直接拉取账号
- 默认保留账号，只过滤明确异常
- 按检查时间降序排列
"""

import re, json, time, hashlib, logging, os
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|"
    r"proton|pm|email|out1ok|live|msn)\.[a-z]{2,}\b",
    re.IGNORECASE)

STATUS_BAD_KEYWORDS = {"异常", "不可用", "失效", "已失效", "暂无可用", "unavailable", "invalid", "error", "失效账号", "暂无"}

def uid(email: str) -> str:
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]

def is_status_bad(status: str) -> bool:
    if not status:
        return False
    s = status.lower().strip()
    return any(kw in s for kw in STATUS_BAD_KEYWORDS)

def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    return driver

def dedup(lst: list) -> list:
    seen, out = set(), []
    for r in lst:
        e = r.get("email","").lower().strip()
        if e and e not in seen:
            seen.add(e); out.append(r)
    return out

def parse_text(text: str) -> list:
    """通用文本解析：内联对 + 上下文关联"""
    results, seen = [], set()
    INLINE = re.compile(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
        r"[\s\t]*(?:密码|password|pwd)?[\s:：/|｜,，\t ]*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)
    CTX_PWD = re.compile(
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)

    for m in INLINE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if (e, p) not in seen and len(p) >= 5 and e not in p:
            seen.add((e, p))
            results.append({"email": e, "password": p, "status": "正常", "checked_at": ""})

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
                if k not in seen and len(k[1]) >= 5:
                    seen.add(k)
                    results.append({"email": k[0], "password": k[1],
                                    "status": "正常", "checked_at": mt.group(1) if mt else ""})
    return results

def scroll_page(driver, times=8, delay=0.6):
    for _ in range(times):
        driver.execute_script("window.scrollBy(0, 700);")
        time.sleep(delay)

JS_EXTRACT = """
    var out=[];
    var done=new Set();
    document.querySelectorAll('*').forEach(function(el){
        var txt=el.innerText||'';
        if(!txt.includes('@')||txt.length>4000||txt.length<10) return;
        var em=txt.match(/[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i);
        if(!em) return;
        var email=em[0].toLowerCase();
        if(done.has(email)) return;
        var pwd='';
        el.querySelectorAll('input').forEach(function(inp){
            var v=inp.value||inp.getAttribute('value')||inp.defaultValue||'';
            if(v&&v.length>=5&&!v.includes('@')) pwd=v;
        });
        if(!pwd){
            var m=txt.match(/密[码碼][：:\\s*•·]+([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})/);
            if(m) pwd=m[1];
        }
        if(!pwd){
            var after=txt.slice(txt.indexOf(em[0])+em[0].length);
            var m2=after.match(/[^\\S\\n]*([A-Za-z0-9!@#$%^&*\-_=+]{6,24})[^\\S\\n]/);
            if(!m2) m2=after.match(/\\b([A-Za-z0-9!@#$%^&*\-_=+]{6,24})\\b/);
            if(m2) pwd=m2[1];
        }
        var tm=txt.match(/20\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d/);
        if(email&&pwd&&pwd.length>=5){
            done.add(email);
            out.push({email:email,pwd:pwd,time:tm?tm[0]:''});
        }
    });
    return out;
"""

def js_extract(driver) -> list:
    try:
        data = driver.execute_script(JS_EXTRACT)
        results = []
        for d in (data or []):
            if d.get("email") and d.get("pwd") and len(d["pwd"]) >= 5:
                results.append({"email": d["email"].lower(), "password": d["pwd"],
                                "status": "正常", "checked_at": d.get("time","")})
        return results
    except Exception:
        return []

# ══════════════════════════════════════════════════════════════
#  1. dongyubin/Free-AppleId-Serve  (GitHub API)
# ══════════════════════════════════════════════════════════════
def crawl_dongyubin_github_api(driver) -> list:
    results = []
    try:
        api = requests.get(
            "https://api.github.com/repos/dongyubin/Free-AppleId-Serve/contents",
            headers={**HEADERS, "Accept": "application/vnd.github.v3+json"},
            timeout=12
        )
        files = []
        if api.status_code == 200:
            for f in api.json():
                if f.get("type") == "file":
                    files.append(f.get("download_url",""))
        # 也加几个已知路径
        files += [
            "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/main/appleid.json",
            "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/main/data.json",
            "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/main/README.md",
            "https://raw.githubusercontent.com/dongyubin/Free-AppleId-Serve/main/index.md",
        ]
        for url in files:
            if not url: continue
            try:
                r = requests.get(url, headers=HEADERS, timeout=10)
                if r.status_code != 200: continue
                try:
                    jdata = r.json()
                    accs = jdata if isinstance(jdata, list) else jdata.get("accounts", jdata.get("data", []))
                    for item in (accs or []):
                        e = item.get("email","") or item.get("account","") or item.get("username","")
                        p = item.get("password","") or item.get("pwd","")
                        s = item.get("status","正常")
                        t = item.get("checked_at","") or item.get("time","")
                        if e and p and "@" in e and len(p) >= 4 and not is_status_bad(s):
                            results.append({"email": e.lower(), "password": p, "status": "正常", "checked_at": t})
                except Exception:
                    parsed = [p for p in parse_text(r.text) if not is_status_bad(p.get("status",""))]
                    results.extend(parsed)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"  dongyubin失败: {e}")
    logger.info(f"  dongyubin.github 共得 {len(results)} 条")
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  2. shadowsockshelp.github.io  (requests)
# ══════════════════════════════════════════════════════════════
def crawl_github_shadowsocks(driver) -> list:
    try:
        r = requests.get(
            "https://shadowsockshelp.github.io/Shadowsocks/appleid.html",
            headers=HEADERS, timeout=15)
        if r.status_code != 200: return []
        results = [p for p in parse_text(BeautifulSoup(r.text,"html.parser").get_text("\n"))
                   if not is_status_bad(p.get("status",""))]
        logger.info(f"  shadowsockshelp.github.io 得 {len(results)} 条")
        return dedup(results)
    except Exception as e:
        logger.error(f"  shadowsockshelp失败: {e}"); return []

# ══════════════════════════════════════════════════════════════
#  3. appledi.github.io  (requests)
# ══════════════════════════════════════════════════════════════
def crawl_appledi_github(driver) -> list:
    try:
        r = requests.get("https://appledi.github.io/", headers=HEADERS, timeout=15)
        if r.status_code != 200: return []
        results = [p for p in parse_text(BeautifulSoup(r.text,"html.parser").get_text("\n"))
                   if not is_status_bad(p.get("status",""))]
        logger.info(f"  appledi.github.io 得 {len(results)} 条")
        return dedup(results)
    except Exception as e:
        logger.error(f"  appledi.github.io失败: {e}"); return []

# ══════════════════════════════════════════════════════════════
#  4. ccbaohe.com/appleID/  (Selenium, 有100+账号)
# ══════════════════════════════════════════════════════════════
def crawl_ccbaohe(driver) -> list:
    results = []
    for url in ["https://ccbaohe.com/appleID/", "https://ccbaohe.com/appleID2/"]:
        logger.info(f"  ccbaohe: {url}")
        try:
            driver.get(url)
            time.sleep(5)
            scroll_page(driver, 8)
            r1 = js_extract(driver)
            if not r1:
                soup = BeautifulSoup(driver.page_source, "html.parser")
                r1 = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
            results.extend(r1)
            logger.info(f"    {url} 得 {len(r1)} 条")
        except Exception as e:
            logger.error(f"  ccbaohe {url} 失败: {e}")
        time.sleep(2)
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  5. shadowrocket.best  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_shadowrocket_best(driver) -> list:
    driver.get("https://shadowrocket.best/")
    time.sleep(5)
    scroll_page(driver, 10)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  6. free.iosapp.icu  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_free_iosapp_icu(driver) -> list:
    driver.get("https://free.iosapp.icu/")
    time.sleep(5)
    scroll_page(driver, 5)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  7. idfree.top  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_idfree_top(driver) -> list:
    driver.get("https://idfree.top/")
    time.sleep(4)
    try:
        btn = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH,
            "//button[contains(.,'我已阅读') or contains(.,'继续查看') or contains(.,'查看账号') or contains(.,'确认')]")))
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
    except Exception: pass
    scroll_page(driver, 5)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  8. id.btvda.top  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_id_btvda_top(driver) -> list:
    driver.get("https://id.btvda.top/")
    time.sleep(5)
    scroll_page(driver, 5)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  9. idshare001.me  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_idshare001(driver) -> list:
    driver.get("https://idshare001.me/goso.html")
    time.sleep(5)
    scroll_page(driver, 5)
    results = []
    try:
        data = driver.execute_script("""
            var out=[];
            document.querySelectorAll('[data-account],[data-email],[data-id],[data-username],[data-copy]').forEach(function(el){
                var email=el.getAttribute('data-account')||el.getAttribute('data-email')||
                          el.getAttribute('data-id')||el.getAttribute('data-username')||'';
                var pwd=el.getAttribute('data-password')||el.getAttribute('data-pwd')||
                        el.getAttribute('data-copy')||'';
                if(email&&email.includes('@')&&pwd&&pwd.length>=4) out.push({email:email,pwd:pwd});
            });
            return out;
        """)
        for d in (data or []):
            results.append({"email": d["email"].lower(), "password": d["pwd"],
                            "status": "正常", "checked_at": ""})
    except Exception: pass
    if not results:
        results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  10. app.iosr.cn  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_app_iosr_cn(driver) -> list:
    driver.get("https://app.iosr.cn/tools/apple-shared-id")
    time.sleep(6)
    try:
        driver.find_element(By.XPATH,"//button[contains(.,'刷新') or contains(.,'获取')]").click()
        time.sleep(3)
    except Exception: pass
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  11. id.bocchi2b.top  (Selenium + 弹窗)
# ══════════════════════════════════════════════════════════════
def crawl_bocchi2b(driver) -> list:
    driver.get("https://id.bocchi2b.top/")
    time.sleep(4)
    for sel in ["//button[text()='Ok']","//button[text()='OK']","//button[text()='确认']",
                "//button[contains(@class,'ok')]","//div[contains(@class,'modal')]//button",
                "//button[contains(.,'关闭')]","//button[contains(.,'我知道')]"]:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1); break
        except Exception: pass
    scroll_page(driver, 8)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  12. 139.196.183.52/share/DZhBvnglEU  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_ip_share(driver) -> list:
    driver.get("http://139.196.183.52/share/DZhBvnglEU")
    time.sleep(5)
    scroll_page(driver, 5)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  13. nodeba.com  (Selenium + 进文章)
# ══════════════════════════════════════════════════════════════
def crawl_nodeba(driver) -> list:
    driver.get("https://nodeba.com/")
    time.sleep(5)
    results = []
    try:
        links = driver.find_elements(By.CSS_SELECTOR,
            "article a, h1 a, h2 a, h3 a, .post-title a, .entry-title a, a[href*='nodeba.com']")
        article_url = None
        for link in links:
            href = link.get_attribute("href") or ""
            txt  = (link.text or "").strip()
            if "nodeba.com" in href and href.rstrip("/") not in ("https://nodeba.com","http://nodeba.com") and \
               any(kw in txt for kw in ["Apple","apple","ID","账号","共享","苹果","apple id"]):
                article_url = href; break
        if not article_url:
            for link in links[:5]:
                h = link.get_attribute("href") or ""
                if "nodeba.com" in h and h.rstrip("/") not in ("https://nodeba.com","http://nodeba.com"):
                    article_url = h; break
        logger.info(f"  nodeba文章: {article_url}")
        if article_url:
            driver.get(article_url)
            time.sleep(5)
            scroll_page(driver, 4)
            results = js_extract(driver)
            if not results:
                soup = BeautifulSoup(driver.page_source, "html.parser")
                results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    except Exception as e:
        logger.error(f"  nodeba失败: {e}")
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  14. tkbaohe.com/Shadowrocket/  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_tkbaohe(driver) -> list:
    driver.get("https://tkbaohe.com/Shadowrocket/")
    time.sleep(6)
    scroll_page(driver, 6)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  15. ios.aneeo.com  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_aneeo(driver) -> list:
    driver.get("https://ios.aneeo.com/")
    time.sleep(5)
    scroll_page(driver, 5)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  16. clashid.com.cn  (Selenium)
# ══════════════════════════════════════════════════════════════
def crawl_clashid(driver) -> list:
    driver.get("https://clashid.com.cn/shadowrocket-apple-id")
    time.sleep(5)
    scroll_page(driver, 5)
    results = js_extract(driver)
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  站点列表
# ══════════════════════════════════════════════════════════════
SITES = [
    {"name": "dongyubin.github(API)",     "fn": crawl_dongyubin_github_api},
    {"name": "shadowsockshelp.github.io", "fn": crawl_github_shadowsocks},
    {"name": "appledi.github.io",         "fn": crawl_appledi_github},
    {"name": "ccbaohe.com/appleID",       "fn": crawl_ccbaohe},
    {"name": "shadowrocket.best",         "fn": crawl_shadowrocket_best},
    {"name": "free.iosapp.icu",           "fn": crawl_free_iosapp_icu},
    {"name": "idfree.top",               "fn": crawl_idfree_top},
    {"name": "id.btvda.top",             "fn": crawl_id_btvda_top},
    {"name": "idshare001.me",            "fn": crawl_idshare001},
    {"name": "app.iosr.cn",              "fn": crawl_app_iosr_cn},
    {"name": "id.bocchi2b.top",          "fn": crawl_bocchi2b},
    {"name": "139.196.183.52",           "fn": crawl_ip_share},
    {"name": "nodeba.com",               "fn": crawl_nodeba},
    {"name": "tkbaohe.com",              "fn": crawl_tkbaohe},
    {"name": "ios.aneeo.com",            "fn": crawl_aneeo},
    {"name": "clashid.com.cn",           "fn": crawl_clashid},
]

# ══════════════════════════════════════════════════════════════
#  主逻辑
# ══════════════════════════════════════════════════════════════
def crawl_all() -> dict:
    seen:         dict = {}
    source_stats: dict = {}

    logger.info("启动浏览器...")
    driver = make_driver()
    try:
        for site in SITES:
            logger.info(f"▶ 抓取: {site['name']}")
            try:
                pairs = site["fn"](driver)
            except Exception as e:
                logger.error(f"  站点异常 {site['name']}: {e}")
                pairs = []

            new_count = 0
            now_iso = datetime.now(timezone.utc).isoformat()
            for p in pairs:
                email = p.get("email","").strip().lower()
                pwd   = p.get("password","").strip()
                if not email or not pwd or "@" not in email or len(pwd) < 4:
                    continue
                if len(set(pwd)) < 2:
                    continue
                if pwd.lower() in {"password","12345678","123456","abcdefgh","qwerty123"}:
                    continue
                # 过滤日期格式密码，如 2026-03-13
                if re.match(r'^20\d\d[-/]\d\d[-/]\d\d$', pwd):
                    continue
                # 过滤纯数字且长度<=6或恰好8位全数字（常见日期误匹配）
                if re.match(r'^20\d{6}$', pwd):
                    continue
                if email not in seen:
                    seen[email] = {
                        "id":         uid(email),
                        "email":      email,
                        "password":   pwd,
                        "status":     "正常",
                        "checked_at": p.get("checked_at", ""),
                        "source":     site["name"],
                        "updated_at": now_iso,
                    }
                    new_count += 1

            source_stats[site["name"]] = new_count
            logger.info(f"  → 新增 {new_count} 条（去重后共 {len(seen)} 条）")
            time.sleep(1)
    finally:
        driver.quit()
        logger.info("浏览器已关闭")

    def sort_key(a):
        t = a.get("checked_at","") or a.get("updated_at","")
        return t

    accounts = sorted(seen.values(), key=sort_key, reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total":         len(accounts),
        "source_stats":  source_stats,
        "accounts":      accounts,
    }

if __name__ == "__main__":
    output_path = os.environ.get("OUTPUT_FILE", "apple_ids.json")
    result = crawl_all()
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 完成！共输出 {result['total']} 条账号 → {output_path}")
