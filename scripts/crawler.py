#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 (全站加强版 v3)
- 对每个站点采用专属 + 通用双重策略
- idfree.top / idshare001.me / bocchi2b 三站点深度重写
- 国家信息全面提取，留空不强制美国
- close_popups / enrich_country_time 统一工具函数
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

# 全球地区关键词
COUNTRY_RE = r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古|未知)"

# 国家JS字符串（给JS内联用，不含括号）
COUNTRY_JS = "美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古|未知"

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|"
    r"proton|pm|email|out1ok|live|msn)\.[a-z]{2,}\b",
    re.IGNORECASE)

EMAIL_BROAD = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}"

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
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
    return driver


def dedup(lst):
    seen, out = set(), []
    for r in lst:
        e = r.get("email", "").lower().strip()
        if e and e not in seen:
            seen.add(e)
            out.append(r)
    return out


def parse_text(text):
    results, seen = [], set()
    INLINE = re.compile(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
        r"[\s\t]*(?:密码|password|pwd)?[\s::/|｜,，\t ]*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})", re.IGNORECASE)
    CTX_PWD = re.compile(
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)
    for m in INLINE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if (e, p) not in seen and len(p) >= 5:
            seen.add((e, p))
            results.append({"email": e, "password": p, "status": "正常", "checked_at": "", "country": ""})
    lines = text.splitlines()
    for i, line in enumerate(lines):
        emails = EMAIL_RE.findall(line)
        if not emails: continue
        ctx = "\n".join(lines[max(0, i-2):i+5])
        m = CTX_PWD.search(ctx)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        if m:
            for e in emails:
                k = (e.lower(), m.group(1).strip())
                if k not in seen and len(k[1]) >= 5:
                    seen.add(k)
                    results.append({"email": k[0], "password": k[1],
                                    "status": "正常", "checked_at": mt.group(1) if mt else "", "country": ""})
    return results


def scroll(driver, n=8):
    for _ in range(n):
        driver.execute_script("window.scrollBy(0,700);")
        time.sleep(0.6)


def close_popups(driver):
    """尝试关闭各类弹窗/遮罩，多轮扫描"""
    selectors = [
        "//button[contains(.,'我已阅读')]",
        "//button[contains(.,'继续查看')]",
        "//button[contains(.,'查看账号')]",
        "//button[contains(.,'知道了')]",
        "//button[contains(.,'我知道了')]",
        "//button[contains(.,'同意')]",
        "//button[contains(.,'确认')]",
        "//button[contains(.,'确定')]",
        "//button[contains(.,'关闭')]",
        "//button[contains(.,'Close')]",
        "//button[contains(.,'close')]",
        "//button[text()='Ok']",
        "//button[text()='OK']",
        "//a[contains(.,'我知道了')]",
        "//a[contains(.,'知道了')]",
        "//div[contains(@class,'modal')]//button",
        "//div[contains(@class,'dialog')]//button",
        "//div[contains(@class,'popup')]//button",
        "//div[contains(@class,'notice')]//button",
        "//div[contains(@class,'overlay')]//button",
        "//div[contains(@class,'mask')]//button",
        "//*[@aria-label='Close']",
        "//*[@aria-label='close']",
        "//*[contains(@class,'close-btn')]",
        "//*[contains(@class,'closeBtn')]",
        "//*[contains(@class,'modal-close')]",
    ]
    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.8)
        except Exception:
            pass


def from_inputs(driver):
    try:
        data = driver.execute_script(r"""
var out=[];
document.querySelectorAll('input').forEach(function(inp){
    var v=inp.value||'';
    if(v&&v.length>=5&&!v.includes('@')){
        var p=inp.closest('[class]')||inp.parentElement;
        var txt=p?p.innerText:'';
        var em=txt.match(/[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}/i);
        if(em) out.push({email:em[0],pwd:v,txt:txt});
    }
});
return out;
        """)
        results, seen = [], set()
        for d in (data or []):
            e = d.get("email", "").lower()
            p = d.get("pwd", "")
            if e and p and "@" in e and e not in seen and len(p) >= 5:
                seen.add(e)
                txt = d.get("txt", "")
                mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", txt)
                mc = re.search(COUNTRY_RE, txt)
                results.append({"email": e, "password": p, "status": "正常",
                                 "checked_at": mt.group(1) if mt else "",
                                 "country": mc.group(1) if mc else ""})
        return results
    except Exception:
        return []


def generic_parse(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div", "li", "article", "section", "tr"], recursive=True):
        text = card.get_text(" ", strip=True)
        if len(text) < 15: continue
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
        mc = re.search(COUNTRY_RE, text)
        results.append({"email": me.group().lower(), "password": pwd,
                         "status": "正常", "checked_at": mt.group(1) if mt else "",
                         "country": mc.group(1) if mc else ""})
    return dedup(results)


# JS hook 拦截剪贴板
HOOK_JS = r"""
window.__copied = window.__copied || [];
try {
    var _orig = navigator.clipboard.writeText.bind(navigator.clipboard);
    navigator.clipboard.writeText = function(text){
        window.__copied.push(text);
        return _orig(text);
    };
} catch(e) {}
document.addEventListener('copy', function(e){
    try{
        var t = e.clipboardData && e.clipboardData.getData('text');
        if(t) window.__copied.push(t);
    }catch(ex){}
}, true);
"""


def click_all_copy_btns(driver, max_clicks=300):
    driver.execute_script(HOOK_JS)
    time.sleep(0.5)

    xpath = (
        "//button[contains(.,'复制账号') or contains(.,'复制帐号') or contains(.,'账号')]"
        " | //button[contains(.,'复制密码') or contains(.,'密码')]"
        " | //button[contains(.,'Copy') or contains(.,'copy')]"
        " | //button[contains(.,'复制') and not(contains(.,'链接')) and not(contains(.,'地址'))]"
        " | //a[contains(.,'复制账号') or contains(.,'复制密码')]"
        " | //*[@data-clipboard-target or @data-clipboard-text]"
    )
    btns = driver.find_elements(By.XPATH, xpath)

    emails, pwds = [], []
    for btn in btns[:max_clicks]:
        try:
            before = len(driver.execute_script("return window.__copied||[]"))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.12)
            after = driver.execute_script("return window.__copied||[]")
            if len(after) > before:
                val = after[-1].strip()
                if "@" in val:
                    emails.append(val.lower())
                elif len(val) >= 5:
                    pwds.append(val)
        except Exception:
            pass

    results = []
    seen = set()

    # 策略1：emails[i] + pwds[i] 顺序配对
    for i in range(len(emails)):
        e = emails[i]
        p = pwds[i] if i < len(pwds) else ""
        if e and p and e not in seen and len(p) >= 5:
            seen.add(e)
            results.append({"email": e, "password": p, "status": "正常", "checked_at": "", "country": ""})

    # 策略2：__copied 整体顺序配对
    if not results:
        copied = driver.execute_script("return window.__copied||[]")
        i = 0
        while i < len(copied) - 1:
            a, b = copied[i].strip(), copied[i+1].strip()
            if "@" in a and len(b) >= 5 and "@" not in b:
                e = a.lower()
                if e not in seen:
                    seen.add(e)
                    results.append({"email": e, "password": b, "status": "正常",
                                    "checked_at": "", "country": ""})
                i += 2
            else:
                i += 1

    return results


def enrich_country_time(driver, results):
    """通用：用JS从页面补充每个账号的国家/时间信息"""
    if not results:
        return results
    try:
        js_data = driver.execute_script("""
var CPAT = new RegExp('(""" + COUNTRY_JS + r""")', 'u');
var TPAT = /(20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2})/;
var out = [], seen = {};
document.querySelectorAll('[class],[id]').forEach(function(el){
    var t = el.innerText || el.textContent || '';
    if(t.length < 10 || t.length > 1000) return;
    var em = t.match(/[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}/i);
    if(!em) return;
    var e = em[0].toLowerCase();
    if(seen[e]) return;
    var ct = t.match(CPAT);
    var tm = t.match(TPAT);
    if(ct || tm){
        seen[e] = 1;
        out.push({email: e, country: ct ? ct[1] : "", time: tm ? tm[1] : ""});
    }
});
return out;
        """)
        cmap = {}
        for d in (js_data or []):
            e = d.get("email", "")
            if e and e not in cmap:
                cmap[e] = (d.get("country", ""), d.get("time", ""))
        for r in results:
            info = cmap.get(r.get("email", ""), ("", ""))
            if info[0]: r["country"] = info[0]
            if info[1]: r["checked_at"] = info[1]
    except Exception as ex:
        logger.debug(f"enrich_country_time error: {ex}")
    return results


def js_full_scan(driver, extra_country=""):
    """通用：JS全量扫描页面所有元素，提取邮箱+密码"""
    country_pat = extra_country or COUNTRY_JS
    try:
        raw = driver.execute_script(r"""
var EMAIL_P = /[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|hotmail|outlook|yahoo|live|msn)\.[a-z]{2,}/i;
var PWD_P = /密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})/;
var CPAT = new RegExp('(""" + country_pat + r""")', 'u');
var TPAT = /(20\d{2}-\d{2}-\d{2}[\s]\d{2}:\d{2})/;
var out = [], seen = {};
document.querySelectorAll('*').forEach(function(el){
    if(['SCRIPT','STYLE','HEAD','NOSCRIPT'].includes(el.tagName)) return;
    var t = (el.innerText || '').trim();
    if(t.length < 15 || t.length > 1000) return;
    var em = t.match(EMAIL_P);
    if(!em) return;
    var e = em[0].toLowerCase();
    if(seen[e]) return;
    var pw = t.match(PWD_P);
    var ct = t.match(CPAT);
    var tm = t.match(TPAT);
    if(pw){ seen[e]=1; out.push({email:e, pwd:pw[1], country:ct?ct[1]:"", time:tm?tm[1]:""}); }
});
return out;
        """)
        results, seen = [], set()
        for d in (raw or []):
            e = (d.get("email", "")).lower()
            p = d.get("pwd", "")
            if e and p and "@" in e and len(p) >= 5 and e not in seen:
                seen.add(e)
                results.append({"email": e, "password": p, "status": "正常",
                                 "checked_at": d.get("time", ""),
                                 "country": d.get("country", "")})
        return results
    except Exception as ex:
        logger.debug(f"js_full_scan error: {ex}")
        return []



# ══════════════════════════════════════════════════════════════════
# 核心解析引擎：data-clipboard-text + Cloudflare 邮件保护 + onclick copy()
# ══════════════════════════════════════════════════════════════════

def decode_cfemail(encoded: str) -> str:
    """解码 Cloudflare data-cfemail 属性（XOR算法）"""
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""


def parse_clipboard_site(html: str) -> list:
    """
    解析使用 data-clipboard-text 按钮的账号分享页面。
    适用于 139.196.183.52/share/* 等同类框架站点。
    """
    soup = BeautifulSoup(html, "lxml")
    results = []
    cards = soup.select(".card-body")
    if not cards:
        seen_parents = []
        for btn in soup.select("[data-clipboard-text]"):
            p = btn.find_parent(class_=lambda c: c and any(k in c for k in ("col", "card", "item", "account")))
            if p and p not in seen_parents:
                seen_parents.append(p)
        cards = seen_parents if seen_parents else [soup]

    for card in cards:
        email = ""
        for sel in [".copy-btn", "[id^='username_']", "button.btn-primary[data-clipboard-text]"]:
            btn = card.select_one(sel)
            if btn:
                v = btn.get("data-clipboard-text", "").strip().lower()
                if v and "@" in v:
                    email = v
                    break
        if not email:
            cf = card.select_one(".__cf_email__")
            if cf:
                email = decode_cfemail(cf.get("data-cfemail", "")).lower()
        if not email:
            m = EMAIL_RE.search(card.get_text(" ", strip=True))
            if m:
                email = m.group().lower()
        if not email or "@" not in email:
            continue

        password = ""
        for sel in [".copy-pass-btn", "[id^='password_']", "button.btn-success[data-clipboard-text]"]:
            btn = card.select_one(sel)
            if btn:
                v = btn.get("data-clipboard-text", "").strip()
                if v and len(v) >= 4:
                    password = v
                    break
        if not password:
            continue

        badge = card.select_one(".badge")
        if badge and bad(badge.get_text(strip=True)):
            continue

        card_text = card.get_text(" ", strip=True)
        mt = re.search(r"上次检查[：:\s]*(20\d{2}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?)", card_text)
        checked_at = mt.group(1) if mt else ""
        mc = re.search(COUNTRY_RE, card_text)
        results.append({"email": email, "password": password, "status": "正常",
                        "checked_at": checked_at, "country": mc.group(1) if mc else ""})
    return results


def fetch_and_parse(url: str, driver=None, selenium_wait: int = 6) -> list:
    """
    通用抓取：requests 优先，失败则 Selenium；
    优先用 parse_clipboard_site，再降级到旧方法。
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            results = parse_clipboard_site(resp.text)
            if results:
                logger.info(f"    [requests+clipboard] {url} → {len(results)} 条")
                return results
    except Exception as ex:
        logger.debug(f"    requests 失败 {url}: {ex}")

    if driver is None:
        return []

    try:
        driver.get(url)
        time.sleep(selenium_wait)
        close_popups(driver)
        scroll(driver, n=8)
        time.sleep(1)
        results = parse_clipboard_site(driver.page_source)
        if results:
            logger.info(f"    [selenium+clipboard] {url} → {len(results)} 条")
            return results
        results = click_all_copy_btns(driver)
        if results:
            results = enrich_country_time(driver, results)
            logger.info(f"    [selenium+clipboard_hook] {url} → {len(results)} 条")
            return results
        results = js_full_scan(driver) or from_inputs(driver) or generic_parse(driver)
        logger.info(f"    [selenium+fallback] {url} → {len(results)} 条")
        return results
    except Exception as ex:
        logger.error(f"    fetch_and_parse selenium 异常 {url}: {ex}")
        return []


# ──────────────────────────────────────────
# ccbaohe.com/appleID
# ──────────────────────────────────────────
def crawl_ccbaohe(driver):
    """
    ccbaohe.com 结构：
    - 邮箱：<a class="__cf_email__" style="display:none">邮箱明文</a>（未编码，直接读text）
    - 密码：onclick="copy('密码')"
    - 国家：<span>【国家】</span>
    - 时间：<p class="card-text">检测时间：...</p>
    """
    def _parse_ccbaohe(html):
        soup = BeautifulSoup(html, "lxml")
        results = []
        for card in soup.select(".card-body"):
            # 邮箱：__cf_email__ 这里是明文 href="mailto:xxx"，直接读
            email = ""
            cf = card.select_one(".__cf_email__")
            if cf:
                # 先试 href="mailto:..."
                href = cf.get("href", "")
                if href.startswith("mailto:"):
                    email = href[7:].strip().lower()
                if not email or "@" not in email:
                    # 再试 data-cfemail（编码）
                    encoded = cf.get("data-cfemail", "")
                    if encoded:
                        enc = bytes.fromhex(encoded)
                        key = enc[0]
                        email = "".join(chr(b ^ key) for b in enc[1:]).lower()
                if not email or "@" not in email:
                    # 最后试文本内容（明文显示的情况）
                    t = cf.get_text(strip=True).lower()
                    if "@" in t:
                        email = t
            if not email or "@" not in email:
                continue

            # 密码：从复制密码按钮的 onclick 提取 copy('xxx')
            password = ""
            for btn in card.select("button"):
                txt = btn.get_text(strip=True)
                if "复制密码" in txt or "密码" in txt:
                    oc = btn.get("onclick", "")
                    m = re.search(r"copy\(([^)]{4,64})\)", oc)
                    if not m:
                        m = re.search(r"copy\(&#39;([^&]{4,64})&#39;", oc)
                    if m:
                        password = m.group(1).strip()
                        # strip surrounding quotes if present
                        password = password.strip('"').strip("'")
                        break
                # 也试 data-clipboard-text
                pb = card.select_one("[data-clipboard-text]")
                if pb:
                    password = pb.get("data-clipboard-text","").strip()
            if not password or len(password) < 4:
                continue

            # 状态
            card_text = card.get_text(" ", strip=True)
            if re.search(r"(异常|失效|不可用|锁定|disabled)", card_text, re.I):
                continue

            # 时间
            mt = re.search(r"检测时间[：:\s]*(20\d{2}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?)", card_text)
            checked_at = mt.group(1) if mt else ""

            # 国家（从 header 取）
            header = card.find_previous("div", class_="card-header")
            country = ""
            if header:
                mc = re.search(r"【(" + COUNTRY_RE[1:-1] + r")】", header.get_text())
                if mc:
                    country = mc.group(1)

            results.append({"email": email, "password": password,
                            "status": "正常", "checked_at": checked_at, "country": country})
        return results

    # 优先 requests
    results = []
    try:
        resp = requests.get("https://ccbaohe.com/appleID/", headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            results = _parse_ccbaohe(resp.text)
            logger.info(f"    [requests] ccbaohe → {len(results)} 条")
    except Exception as ex:
        logger.debug(f"    ccbaohe requests 失败: {ex}")

    # Selenium 兜底
    if not results:
        driver.get("https://ccbaohe.com/appleID/")
        time.sleep(8)
        close_popups(driver)
        scroll(driver, n=10)
        time.sleep(2)
        results = _parse_ccbaohe(driver.page_source)
        if not results:
            results = click_all_copy_btns(driver) or js_full_scan(driver) or generic_parse(driver)
        logger.info(f"    [selenium] ccbaohe → {len(results)} 条")

    logger.info(f"  ccbaohe 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# tkbaohe.com/Shadowrocket/
# ──────────────────────────────────────────
def crawl_tkbaohe(driver):
    """tkbaohe.com 结构与 ccbaohe 完全相同，复用同一解析逻辑"""
    def _parse(html):
        soup = BeautifulSoup(html, "lxml")
        results = []
        for card in soup.select(".card-body"):
            email = ""
            cf = card.select_one(".__cf_email__")
            if cf:
                href = cf.get("href", "")
                if href.startswith("mailto:"):
                    email = href[7:].strip().lower()
                if not email or "@" not in email:
                    encoded = cf.get("data-cfemail", "")
                    if encoded:
                        try:
                            enc = bytes.fromhex(encoded)
                            key = enc[0]
                            email = "".join(chr(b ^ key) for b in enc[1:]).lower()
                        except Exception:
                            pass
                if not email or "@" not in email:
                    t = cf.get_text(strip=True).lower()
                    if "@" in t:
                        email = t
            if not email or "@" not in email:
                continue
            password = ""
            for btn in card.select("button"):
                if "密码" in btn.get_text(strip=True):
                    oc = btn.get("onclick", "")
                    m = re.search(r"copy\(([^)]{4,64})\)", oc)
                    if not m:
                        m = re.search(r"copy\(&#39;([^&]{4,64})&#39;", oc)
                    if m:
                        password = m.group(1).strip('"').strip("'")
                        break

                pb = card.select_one("[data-clipboard-text]")
                if pb:
                    password = pb.get("data-clipboard-text", "").strip()
            if not password or len(password) < 4:
                continue
            card_text = card.get_text(" ", strip=True)
            if re.search(r"(异常|失效|不可用|锁定)", card_text):
                continue
            mt = re.search(r"检测时间[：:\s]*(20\d{2}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?)", card_text)
            checked_at = mt.group(1) if mt else ""
            header = card.find_previous("div", class_="card-header")
            country = ""
            if header:
                mc = re.search(COUNTRY_RE, header.get_text())
                if mc:
                    country = mc.group(1)
            results.append({"email": email, "password": password,
                            "status": "正常", "checked_at": checked_at, "country": country})
        return results

    results = []
    try:
        resp = requests.get("https://tkbaohe.com/Shadowrocket/", headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            results = _parse(resp.text)
            logger.info(f"    [requests] tkbaohe → {len(results)} 条")
    except Exception as ex:
        logger.debug(f"    tkbaohe requests 失败: {ex}")

    if not results:
        driver.get("https://tkbaohe.com/Shadowrocket/")
        time.sleep(8)
        close_popups(driver)
        scroll(driver, n=10)
        time.sleep(2)
        results = _parse(driver.page_source)
        if not results:
            results = click_all_copy_btns(driver) or js_full_scan(driver) or generic_parse(driver)
        logger.info(f"    [selenium] tkbaohe → {len(results)} 条")

    logger.info(f"  tkbaohe 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# idfree.top  （之前抓到0）
# ──────────────────────────────────────────
def crawl_idfree_top(driver):
    loaded = False
    for url in ["https://idfree.top/", "https://www.idfree.top/", "https://idfree.top/free"]:
        try:
            driver.get(url)
            WebDriverWait(driver, 12).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            if "@" in driver.page_source or "apple" in driver.page_source.lower():
                loaded = True
                break
        except Exception:
            continue

    if not loaded:
        logger.warning("  idfree.top 页面无有效内容，尝试requests")
        try:
            resp = requests.get("https://idfree.top/", headers=HEADERS, timeout=15)
            results = parse_text(resp.text)
            if results:
                logger.info(f"  idfree_top(requests) 抓到: {len(results)}")
                return dedup(results)
        except Exception:
            pass
        logger.info(f"  idfree_top 抓到: 0")
        return []

    time.sleep(3)
    for _ in range(3):
        close_popups(driver)
        time.sleep(0.5)

    try:
        WebDriverWait(driver, 10).until(lambda d: "@" in d.page_source)
    except Exception:
        pass

    scroll(driver, n=10)
    time.sleep(2)

    results = click_all_copy_btns(driver)
    if not results:
        results = from_inputs(driver)
    if not results:
        results = js_full_scan(driver)
    if not results:
        results = generic_parse(driver)
    if not results:
        try:
            resp = requests.get("https://idfree.top/", headers=HEADERS, timeout=15)
            results = parse_text(resp.text)
        except Exception:
            pass

    logger.info(f"  idfree_top 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# id.btvda.top  （最大来源）
# ──────────────────────────────────────────
def crawl_id_btvda_top(driver):
    driver.get("https://id.btvda.top/")
    time.sleep(6)
    close_popups(driver)
    scroll(driver, n=15)
    time.sleep(2)

    results = click_all_copy_btns(driver)
    results = enrich_country_time(driver, results)
    if not results:
        results = js_full_scan(driver)
    if not results:
        results = from_inputs(driver) or generic_parse(driver)

    logger.info(f"  id_btvda_top 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# idshare001.me  （之前抓到0）
# ──────────────────────────────────────────
def crawl_idshare001(driver):
    loaded = False
    for url in [
        "https://idshare001.me/goso.html",
        "https://idshare001.me/",
        "https://idshare001.me/apple",
        "https://idshare001.me/free",
        "https://idshare001.me/share",
    ]:
        try:
            driver.get(url)
            WebDriverWait(driver, 12).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            src = driver.page_source
            if "@" in src and len(src) > 2000:
                loaded = True
                logger.info(f"  idshare001 有效URL: {url}")
                break
        except Exception:
            continue

    if not loaded:
        logger.warning("  idshare001 所有路径均无效，尝试requests")
        try:
            resp = requests.get("https://idshare001.me/goso.html", headers=HEADERS, timeout=15)
            results = parse_text(resp.text)
            if results:
                logger.info(f"  idshare001(requests) 抓到: {len(results)}")
                return dedup(results)
        except Exception:
            pass
        logger.info(f"  idshare001 抓到: 0")
        return []

    time.sleep(2)
    for _ in range(3):
        close_popups(driver)
        time.sleep(0.5)

    scroll(driver, n=10)
    time.sleep(2)

    results = click_all_copy_btns(driver)

    if not results:
        # data-* 属性 + input value 提取
        try:
            data = driver.execute_script(r"""
var out=[], seen={};
document.querySelectorAll('[data-account],[data-email],[data-password],[data-pwd],[data-id]').forEach(function(el){
    var em=el.getAttribute('data-account')||el.getAttribute('data-email')||el.getAttribute('data-id')||'';
    var pw=el.getAttribute('data-password')||el.getAttribute('data-pwd')||'';
    if(em&&em.includes('@')&&pw&&pw!=='undefined'&&pw.length>=5&&!seen[em.toLowerCase()]){
        seen[em.toLowerCase()]=1;
        out.push({email:em,pwd:pw});
    }
});
document.querySelectorAll('input[type=text],input[type=password],input:not([type])').forEach(function(inp){
    var v=inp.value||inp.getAttribute('value')||'';
    if(!v||v.length<5) return;
    var parent=inp.closest('[class]')||inp.parentElement;
    var txt=parent?(parent.innerText||''):'';
    var em=txt.match(/[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}/i);
    if(em&&!seen[em[0].toLowerCase()]&&v.indexOf('@')<0){
        seen[em[0].toLowerCase()]=1;
        out.push({email:em[0],pwd:v});
    }
});
return out;
            """)
            seen = set()
            for d in (data or []):
                e = (d.get("email") or "").lower()
                p = d.get("pwd") or ""
                if e and p and "@" in e and len(p) >= 5 and e not in seen:
                    seen.add(e)
                    results.append({"email": e, "password": p, "status": "正常", "checked_at": "", "country": ""})
        except Exception as ex:
            logger.debug(f"  idshare001 data-attr: {ex}")

    if not results:
        results = from_inputs(driver)
    if not results:
        results = js_full_scan(driver)
    if not results:
        results = generic_parse(driver)
    if not results:
        try:
            resp = requests.get("https://idshare001.me/goso.html", headers=HEADERS, timeout=15)
            results = parse_text(resp.text)
        except Exception:
            pass

    logger.info(f"  idshare001 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# id.bocchi2b.top  （之前抓到0）
# ──────────────────────────────────────────
def crawl_bocchi2b(driver):
    driver.get("https://id.bocchi2b.top/")
    time.sleep(6)

    # 分多轮关弹窗
    for _ in range(4):
        close_popups(driver)
        time.sleep(0.8)

    # 等账号内容出现
    try:
        WebDriverWait(driver, 15).until(
            lambda d: "@" in d.page_source and len(d.page_source) > 5000
        )
    except Exception:
        pass

    scroll(driver, n=12)
    time.sleep(2)

    results = click_all_copy_btns(driver)
    results = enrich_country_time(driver, results)
    if not results:
        results = from_inputs(driver)
    if not results:
        results = js_full_scan(driver)
    if not results:
        results = generic_parse(driver)
    if not results:
        try:
            resp = requests.get("https://id.bocchi2b.top/", headers=HEADERS, timeout=15)
            results = parse_text(resp.text)
        except Exception:
            pass

    logger.info(f"  bocchi2b 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# shadowrocket.best/
# ──────────────────────────────────────────
def crawl_shadowrocket_best(driver):
    driver.get("https://shadowrocket.best/")
    time.sleep(6)
    close_popups(driver)

    # 多次滚动加载全部卡片
    last_count = 0
    for _ in range(30):
        driver.execute_script("window.scrollBy(0, 600);")
        time.sleep(0.7)
        cards = driver.find_elements(By.CSS_SELECTOR,
            ".card,.id-card,.account-card,[class*='card'],[class*='item'],[class*='account']")
        if len(cards) == last_count:
            break
        last_count = len(cards)
    driver.execute_script("window.scrollTo(0,0)")
    time.sleep(1)

    results = click_all_copy_btns(driver)
    results = enrich_country_time(driver, results)
    if not results:
        results = js_full_scan(driver)
    if not results:
        seen = set()
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for card in soup.find_all(["div", "li"], recursive=True):
            text = card.get_text(" ", strip=True)
            if len(text) < 20 or len(text) > 500: continue
            me = EMAIL_RE.search(text)
            if not me: continue
            e = me.group().lower()
            if e in seen: continue
            mp = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
            if not mp:
                after = text[me.end():]
                mp2 = re.search(r"\b([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})\b", after)
                if not mp2: continue
                pwd = mp2.group(1)
                if re.match(r"^\d{4}-\d{2}-\d{2}$", pwd): continue
            else:
                pwd = mp.group(1)
            mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            mc = re.search(COUNTRY_RE, text)
            seen.add(e)
            results.append({"email": e, "password": pwd, "status": "正常",
                             "checked_at": mt.group(1) if mt else "",
                             "country": mc.group(1) if mc else ""})

    if not results:
        results = from_inputs(driver)
    return dedup(results)


# ──────────────────────────────────────────
# free.iosapp.icu/
# ──────────────────────────────────────────
def crawl_free_iosapp_icu(driver):
    """
    free.iosapp.icu 是纯文本格式，无复制按钮：
      账号: xxx@outlook.com
      密码: 58du&7SC
      状态: 账号可用
      检查时间: 2026-xx-xx xx:xx
    """
    def _parse_iosapp(html):
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        text = soup.get_text("\n", strip=True)
        # 按账号块切割（每个账号都有"账号:"标记）
        blocks = re.split(r"(?=账号[:：])", text)
        for block in blocks:
            me = re.search(r"账号[:：]\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})", block, re.I)
            if not me:
                me = re.search(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})", block, re.I)
            if not me:
                continue
            email = me.group(1).lower()
            if "@" not in email or email in seen:
                continue
            mp = re.search(r"密码[:：]\s*([^\n\s]{4,64})", block)
            if not mp:
                continue
            password = mp.group(1).strip()
            # 检查状态
            ms = re.search(r"状态[:：]\s*(.+)", block)
            status_text = ms.group(1).strip() if ms else ""
            if re.search(r"(异常|失效|不可用|locked|disabled|unavailable)", status_text, re.I):
                continue
            # 时间
            mt = re.search(r"检查时间[:：]?\s*(20\d{2}-\d{2}-\d{2}[\s T]\d{2}:\d{2}(?::\d{2})?)", block)
            checked_at = mt.group(1).strip() if mt else ""
            mc = re.search(COUNTRY_RE, block)
            seen.add(email)
            results.append({"email": email, "password": password,
                            "status": "正常", "checked_at": checked_at,
                            "country": mc.group(1) if mc else ""})
        return results

    results = []
    try:
        resp = requests.get("https://free.iosapp.icu/", headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            results = _parse_iosapp(resp.text)
            logger.info(f"    [requests] free.iosapp.icu → {len(results)} 条")
    except Exception as ex:
        logger.debug(f"    iosapp requests 失败: {ex}")

    if not results:
        driver.get("https://free.iosapp.icu/")
        time.sleep(6)
        close_popups(driver)
        scroll(driver, n=10)
        time.sleep(2)
        results = _parse_iosapp(driver.page_source)
        if not results:
            # 再试剪贴板或js扫描
            results = click_all_copy_btns(driver) or js_full_scan(driver)
        logger.info(f"    [selenium] free.iosapp.icu → {len(results)} 条")

    logger.info(f"  free_iosapp_icu 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# app.iosr.cn/tools/apple-shared-id
# ──────────────────────────────────────────
def crawl_app_iosr_cn(driver):
    """
    app.iosr.cn 有专属结构，需要 Selenium（JS动态渲染）。
    密码通过 JS 直接读取每个账号卡片的所有 data-* 属性和按钮 onclick。
    """
    driver.get("https://app.iosr.cn/tools/apple-shared-id")
    time.sleep(7)
    close_popups(driver)
    try:
        driver.find_element(By.XPATH, "//button[contains(.,'刷新') or contains(.,'refresh')]").click()
        time.sleep(4)
    except Exception:
        pass
    scroll(driver, n=10)
    time.sleep(2)

    # 用 JS 直接扫描页面所有含邮箱的元素块，提取账号+密码
    data = driver.execute_script(r"""
var results = [];
var seen = {};
var EMAIL_P = /[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}/i;
// 找所有含邮箱的卡片容器
var cards = document.querySelectorAll('.card,.item,.account-item,[class*="account"],[class*="id-item"]');
if(cards.length === 0) cards = document.querySelectorAll('[class]');
cards.forEach(function(card) {
    var text = card.innerText || '';
    var emailMatch = text.match(EMAIL_P);
    if(!emailMatch) return;
    var email = emailMatch[0].toLowerCase();
    if(seen[email]) return;
    // 密码策略1: data-clipboard-text
    var pwd = '';
    var clipBtns = card.querySelectorAll('[data-clipboard-text]');
    clipBtns.forEach(function(btn) {
        var v = btn.getAttribute('data-clipboard-text') || '';
        if(v && v.indexOf('@') < 0 && v.length >= 4 && v.length <= 64) pwd = v;
    });
    // 密码策略2: onclick copy('xxx')
    if(!pwd) {
        var btns = card.querySelectorAll('button,a');
        btns.forEach(function(btn) {
            var oc = btn.getAttribute('onclick') || '';
            var m = oc.match(/copy\(['"]([^'"]{4,32})['"]\)/);
            if(m && m[1].indexOf('@') < 0) pwd = m[1];
        });
    }
    // 密码策略3: input value
    if(!pwd) {
        var inputs = card.querySelectorAll('input[type="text"],input[type="password"],input:not([type])');
        inputs.forEach(function(inp) {
            var v = inp.value || inp.getAttribute('value') || '';
            if(v && v.indexOf('@') < 0 && v.length >= 4) pwd = v;
        });
    }
    if(!pwd || pwd.length < 4) return;
    // 检查状态（含"异常"等词则跳过）
    if(/异常|失效|不可用|锁定/.test(text)) return;
    // 时间
    var tm = text.match(/(20\d{2}-\d{2}-\d{2}[\s T]\d{2}:\d{2}(?::\d{2})?)/);
    seen[email] = 1;
    results.push({email: email, pwd: pwd, time: tm ? tm[1].trim() : ''});
});
return results;
    """)
    results = []
    seen = set()
    for d in (data or []):
        e = (d.get("email") or "").lower().strip()
        p = (d.get("pwd") or "").strip()
        if e and p and "@" in e and len(p) >= 4 and e not in seen:
            seen.add(e)
            mc = re.search(COUNTRY_RE, e + (d.get("time") or ""))
            results.append({"email": e, "password": p, "status": "正常",
                            "checked_at": (d.get("time") or "").strip(),
                            "country": mc.group(1) if mc else ""})

    # 如果 JS 扫不到，降级
    if not results:
        results = parse_clipboard_site(driver.page_source) or click_all_copy_btns(driver) or js_full_scan(driver)

    logger.info(f"  app.iosr.cn 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# 139.196.183.52/share/DZhBvnglEU
# ──────────────────────────────────────────
def crawl_ip_share(driver):
    driver.get("http://139.196.183.52/share/DZhBvnglEU")
    time.sleep(6)
    close_popups(driver)
    scroll(driver)

    # 点显示密码按钮
    try:
        btns = driver.find_elements(By.XPATH,
            "//button[contains(.,'复制密码')]|//button[contains(.,'查看密码')]|//button[contains(.,'显示密码')]")
        for btn in btns[:20]:
            try:
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.4)
            except Exception:
                pass
        time.sleep(1)
    except Exception:
        pass

    results = click_all_copy_btns(driver)

    if not results:
        try:
            data = driver.execute_script(r"""
var out=[], seen={};
document.querySelectorAll('[class]').forEach(function(card){
    var t=card.innerText||card.textContent||'';
    var em=t.match(/[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}/i);
    if(!em) return;
    var e=em[0].toLowerCase();
    if(seen[e]) return;
    var inp=card.querySelector('input');
    var pwd=inp?(inp.value||''):'';
    if(!pwd||pwd.length<5){
        var spans=card.querySelectorAll('span,p,div,td');
        for(var i=0;i<spans.length;i++){
            var sv=spans[i].innerText||'';
            if(sv&&sv.length>=5&&sv.length<=32&&!sv.includes('@')&&!/^20\d{2}/.test(sv)){
                pwd=sv.trim(); break;
            }
        }
    }
    var mt=t.match(/上次检查[:：\s]*(20\d{2}-\d{2}-\d{2} \d{2}:\d{2})/);
    var ms=t.match(/(正常|解锁成功|可用)/);
    if(pwd&&pwd.length>=5&&ms){
        seen[e]=1;
        out.push({email:e,pwd:pwd,time:mt?mt[1]:''});
    }
});
return out;
            """)
            seen = set()
            for d in (data or []):
                e = (d.get("email") or "").lower()
                p = (d.get("pwd") or "").strip()
                if e and p and "@" in e and len(p) >= 5 and e not in seen:
                    seen.add(e)
                    results.append({"email": e, "password": p, "status": "正常",
                                    "checked_at": d.get("time", ""), "country": ""})
        except Exception as ex:
            logger.warning(f"  ip_share JS: {ex}")

    if not results:
        results = js_full_scan(driver)
    if not results:
        results = from_inputs(driver) or generic_parse(driver)
    return dedup(results)


SITES = [
    # 按用户指定顺序排列
    {"name": "idshare001.me",        "fn": crawl_idshare001},
    {"name": "idfree.top",           "fn": crawl_idfree_top},
    {"name": "139.196.183.52",       "fn": crawl_ip_share},
    {"name": "free.iosapp.icu",      "fn": crawl_free_iosapp_icu},
    {"name": "app.iosr.cn",          "fn": crawl_app_iosr_cn},
    {"name": "shadowrocket.best",    "fn": crawl_shadowrocket_best},
    {"name": "ccbaohe.com/appleID",  "fn": crawl_ccbaohe},
    {"name": "tkbaohe.com",          "fn": crawl_tkbaohe},
    {"name": "id.btvda.top",         "fn": crawl_id_btvda_top},
    {"name": "id.bocchi2b.top",      "fn": crawl_bocchi2b},
]

# 站点排序权重（顺序越靠前权重越小，用于排序）
SITE_ORDER = {s["name"]: i for i, s in enumerate(SITES)}

def crawl_all():
    seen, source_stats = {}, {}
    logger.info("启动浏览器...")
    driver = make_driver()
    try:
        for site in SITES:
            logger.info(f"▶ 抓取: {site['name']}")
            try:
                pairs = site["fn"](driver)
            except Exception as e:
                logger.error(f"  {site['name']} 异常: {e}")
                pairs = []
            nc = 0
            for p in pairs:
                e = p.get("email", "").strip().lower()
                pw = p.get("password", "").strip()
                if not e or not pw or "@" not in e or len(pw) < 4: continue
                if len(set(pw)) < 2: continue
                if e not in seen:
                    seen[e] = {
                        "id":         uid(e),
                        "email":      e,
                        "password":   pw,
                        "status":     p.get("status", "正常"),
                        "country":    p.get("country", ""),
                        "checked_at": p.get("checked_at", ""),
                        "source":     site["name"],
                        "updated_at": now_cst()
                    }
                    nc += 1
            source_stats[site["name"]] = nc
            logger.info(f"  → 新增 {nc} 条（共 {len(seen)} 条）")
            time.sleep(2)
    finally:
        driver.quit()
        logger.info("浏览器已关闭")

    # 排序：先按站点顺序（用户指定），同站点内按检查时间倒序
    def sort_key(a):
        site_rank = SITE_ORDER.get(a.get("source", ""), 999)
        t = a.get("checked_at", "") or a.get("updated_at", "") or ""
        return (site_rank, t)
    accounts = sorted(seen.values(), key=sort_key)
    return {
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M"),
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
