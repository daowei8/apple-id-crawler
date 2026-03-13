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


# ──────────────────────────────────────────
# ccbaohe.com/appleID
# ──────────────────────────────────────────
def crawl_ccbaohe(driver):
    driver.get("https://ccbaohe.com/appleID")
    time.sleep(8)
    close_popups(driver)
    scroll(driver)
    time.sleep(2)

    results = click_all_copy_btns(driver)
    results = enrich_country_time(driver, results)
    if not results:
        results = js_full_scan(driver)
    if not results:
        results = generic_parse(driver)

    logger.info(f"  ccbaohe 抓到: {len(results)}")
    return dedup(results)


# ──────────────────────────────────────────
# tkbaohe.com/Shadowrocket/
# ──────────────────────────────────────────
def crawl_tkbaohe(driver):
    driver.get("https://tkbaohe.com/Shadowrocket/")
    time.sleep(8)
    close_popups(driver)
    scroll(driver)
    time.sleep(2)

    results = click_all_copy_btns(driver)
    results = enrich_country_time(driver, results)
    if not results:
        results = js_full_scan(driver)
    if not results:
        results = generic_parse(driver)

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
    driver.get("https://free.iosapp.icu/")
    time.sleep(5)
    close_popups(driver)
    scroll(driver)

    results = click_all_copy_btns(driver)
    results = enrich_country_time(driver, results)

    if not results:
        seen = set()
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for block in soup.find_all(["div", "section", "article"], recursive=True):
            text = block.get_text(" ", strip=True)
            if len(text) < 20 or len(text) > 800: continue
            me = re.search(r"账[号号][:：\s]*(" + EMAIL_BROAD + r")", text, re.I)
            if not me:
                me = EMAIL_RE.search(text)
            if not me: continue
            e = (me.group(1) if me.lastindex else me.group()).lower()
            if "@" not in e or e in seen: continue
            mp = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
            if not mp: continue
            pwd = mp.group(1)
            if re.match(r"^20\d\d-\d\d-\d\d$", pwd): continue
            ms = re.search(r"状[态態][:：\s]*(\S+)", text)
            status = ms.group(1) if ms else "正常"
            if bad(status): continue
            mt = re.search(r"检查时间[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            mc = re.search(COUNTRY_RE, text)
            seen.add(e)
            results.append({"email": e, "password": pwd, "status": "正常",
                             "checked_at": mt.group(1) if mt else "",
                             "country": mc.group(1) if mc else ""})

    if not results:
        results = js_full_scan(driver)
    if not results:
        results = from_inputs(driver)
    return dedup(results)


# ──────────────────────────────────────────
# app.iosr.cn/tools/apple-shared-id
# ──────────────────────────────────────────
def crawl_app_iosr_cn(driver):
    driver.get("https://app.iosr.cn/tools/apple-shared-id")
    time.sleep(7)
    close_popups(driver)
    try:
        driver.find_element(By.XPATH, "//button[contains(.,'刷新')]").click()
        time.sleep(4)
    except Exception:
        pass
    scroll(driver)

    results = click_all_copy_btns(driver)
    results = enrich_country_time(driver, results)

    if not results:
        seen = set()
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for card in soup.find_all(["div", "li", "article"], class_=True):
            text = card.get_text(" ", strip=True)
            if len(text) < 20 or len(text) > 600: continue
            me = EMAIL_RE.search(text)
            if not me: continue
            e = me.group().lower()
            if e in seen: continue
            mp = re.search(r"密[码碼][\s:：]*([^\s]{5,32})", text)
            if not mp:
                after = text[me.end():]
                mp2 = re.search(r"\b([A-Za-z0-9!@#$%^&*()\-_=+:]{6,32})\b", after)
                if not mp2: continue
                pwd = mp2.group(1)
            else:
                pwd = mp.group(1)
            if re.match(r"^20\d\d[-/]\d\d[-/]\d\d", pwd): continue
            mt = re.search(r"更新时间[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            if not mt: mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            ms = re.search(r"(正常使用|正常|可用)", text)
            if not ms: continue
            mc = re.search(COUNTRY_RE, text)
            seen.add(e)
            results.append({"email": e, "password": pwd, "status": "正常",
                             "checked_at": mt.group(1) if mt else "",
                             "country": mc.group(1) if mc else ""})

    if not results:
        results = js_full_scan(driver)
    if not results:
        results = from_inputs(driver)
    return dedup(results)


# ──────────────────────────────────────────────────────────────────
# Cloudflare 邮件解码工具
# 该站点使用 Cloudflare 邮件保护，邮箱被编码为 data-cfemail 属性
# ──────────────────────────────────────────────────────────────────
def decode_cfemail(encoded: str) -> str:
    """解码 Cloudflare data-cfemail 属性"""
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""


def parse_clipboard_site(html: str, source_url: str = "") -> list:
    """
    解析使用 data-clipboard-text 按钮 + Cloudflare 邮件保护的账号分享页面。
    适用于 139.196.183.52/share/* 系列以及同类框架搭建的站点。

    策略：
    1. 找每张账号卡片（card-body）
    2. 账号：优先读 copy-btn 的 data-clipboard-text，其次解码 __cf_email__
    3. 密码：读 copy-pass-btn 的 data-clipboard-text
    4. 状态：读 badge 文字，跳过异常账号
    5. 检查时间：读"上次检查"文字
    """
    soup = BeautifulSoup(html, "lxml")
    results = []

    # 找所有账号卡片：card-body 是最通用的容器
    cards = soup.select(".card-body")
    if not cards:
        # 降级：找所有含复制按钮的父容器
        cards = []
        for btn in soup.select("[data-clipboard-text]"):
            parent = btn.find_parent(class_=lambda c: c and "col" in c)
            if parent and parent not in cards:
                cards.append(parent)

    for card in cards:
        # ── 提取邮箱 ──
        email = ""
        # 方法1：copy-btn 的 data-clipboard-text（最可靠）
        copy_btn = card.select_one(".copy-btn, [id^='username_']")
        if copy_btn:
            email = copy_btn.get("data-clipboard-text", "").strip().lower()

        # 方法2：解码 Cloudflare 保护的邮箱
        if not email or "@" not in email:
            cf = card.select_one(".__cf_email__")
            if cf:
                encoded = cf.get("data-cfemail", "")
                email = decode_cfemail(encoded).lower()

        # 方法3：正则从文本中提取（兜底）
        if not email or "@" not in email:
            text = card.get_text(" ", strip=True)
            m = EMAIL_RE.search(text)
            if m:
                email = m.group().lower()

        if not email or "@" not in email:
            continue

        # ── 提取密码 ──
        password = ""
        pass_btn = card.select_one(".copy-pass-btn, [id^='password_']")
        if pass_btn:
            password = pass_btn.get("data-clipboard-text", "").strip()

        if not password or len(password) < 4:
            continue

        # ── 提取状态 ──
        status = "正常"
        badge = card.select_one(".badge")
        if badge:
            status = badge.get_text(strip=True)
        if bad(status):
            logger.debug(f"  跳过异常账号: {email} 状态={status}")
            continue

        # ── 提取检查时间 ──
        checked_at = ""
        card_text = card.get_text(" ", strip=True)
        mt = re.search(r"上次检查[：:\s]*(20\d{2}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?)", card_text)
        if mt:
            checked_at = mt.group(1)

        # ── 提取国家 ──
        country = ""
        mc = re.search(COUNTRY_RE, card_text)
        if mc:
            country = mc.group(1)

        results.append({
            "email": email,
            "password": password,
            "status": "正常",
            "checked_at": checked_at,
            "country": country,
        })

    return results


def crawl_clipboard_site(driver, url: str, site_name: str = "") -> list:
    """
    通用爬虫：适用于所有使用 data-clipboard-text 按钮的账号分享站点。
    支持多页（如有分页则自动翻页）。
    """
    logger.info(f"  [clipboard_site] 加载: {url}")
    driver.get(url)
    time.sleep(5)
    close_popups(driver)
    scroll(driver, n=6)

    all_results = []
    visited_pages = set()
    page_num = 0

    while True:
        current_url = driver.current_url
        if current_url in visited_pages:
            break
        visited_pages.add(current_url)
        page_num += 1

        html = driver.page_source
        page_results = parse_clipboard_site(html, current_url)
        logger.info(f"    第{page_num}页: 解析到 {len(page_results)} 条账号")
        all_results.extend(page_results)

        # 尝试翻页
        next_btn = None
        for selector in ["a[rel='next']", ".pagination .next a", "a:contains('下一页')",
                          "//a[contains(.,'下一页') or contains(.,'Next') or contains(.,'›')]"]:
            try:
                if selector.startswith("//"):
                    elems = driver.find_elements(By.XPATH, selector)
                else:
                    elems = driver.find_elements(By.CSS_SELECTOR, selector)
                if elems:
                    next_btn = elems[0]
                    break
            except Exception:
                pass

        if not next_btn:
            break
        try:
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(3)
        except Exception:
            break

    return dedup(all_results)


# ──────────────────────────────────────────────────────────────────
# 139.196.183.52 系列（已知页面 + 自动发现同域其他分享页）
# ──────────────────────────────────────────────────────────────────
IP_SHARE_URLS = [
    "http://139.196.183.52/share/DZhBvnglEU",
    # 可在此追加同域其他分享 URL，爬虫也会从首页自动发现
]

def crawl_ip_share(driver) -> list:
    """
    抓取 139.196.183.52 账号分享站。
    1. 先尝试直接请求（requests，速度快）
    2. requests 拿到的 HTML 就用 parse_clipboard_site 解析
    3. 若解析结果为空则用 Selenium 兜底
    4. 自动从首页或已知路径探测更多分享页面
    """
    all_results = []
    discovered_urls = set(IP_SHARE_URLS)

    # ── 步骤1：requests 快速抓取已知 URL ──
    for url in list(discovered_urls):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.encoding = "utf-8"
            if resp.status_code == 200:
                results = parse_clipboard_site(resp.text, url)
                logger.info(f"  [requests] {url} → {len(results)} 条")
                if results:
                    all_results.extend(results)
                    continue
            # requests 失败或结果为空 → Selenium 兜底
        except Exception as ex:
            logger.warning(f"  [requests] {url} 失败: {ex}")

        # Selenium 兜底
        try:
            results = crawl_clipboard_site(driver, url, "139.196.183.52")
            logger.info(f"  [selenium] {url} → {len(results)} 条")
            all_results.extend(results)
        except Exception as ex:
            logger.error(f"  [selenium] {url} 异常: {ex}")

    # ── 步骤2：尝试自动发现首页上的其他分享链接 ──
    try:
        index_urls = [
            "http://139.196.183.52/",
            "http://139.196.183.52/share",
        ]
        for idx_url in index_urls:
            try:
                resp = requests.get(idx_url, headers=HEADERS, timeout=10)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/share/" in href:
                        full = href if href.startswith("http") else f"http://139.196.183.52{href}"
                        if full not in discovered_urls:
                            discovered_urls.add(full)
                            logger.info(f"  [发现新页面] {full}")
                            try:
                                r2 = requests.get(full, headers=HEADERS, timeout=15)
                                r2.encoding = "utf-8"
                                if r2.status_code == 200:
                                    new_results = parse_clipboard_site(r2.text, full)
                                    logger.info(f"    → {len(new_results)} 条")
                                    all_results.extend(new_results)
                            except Exception as ex2:
                                logger.warning(f"    新页面失败: {ex2}")
            except Exception:
                pass
    except Exception as ex:
        logger.warning(f"  首页探测失败: {ex}")

    return dedup(all_results)


SITES = [
    {"name": "ccbaohe.com/appleID",  "fn": crawl_ccbaohe},
    {"name": "shadowrocket.best",    "fn": crawl_shadowrocket_best},
    {"name": "free.iosapp.icu",      "fn": crawl_free_iosapp_icu},
    {"name": "idfree.top",           "fn": crawl_idfree_top},
    {"name": "id.btvda.top",         "fn": crawl_id_btvda_top},
    {"name": "idshare001.me",        "fn": crawl_idshare001},
    {"name": "app.iosr.cn",          "fn": crawl_app_iosr_cn},
    {"name": "id.bocchi2b.top",      "fn": crawl_bocchi2b},
    {"name": "139.196.183.52",       "fn": crawl_ip_share},
    {"name": "tkbaohe.com",          "fn": crawl_tkbaohe},
]


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

    accounts = sorted(seen.values(),
                      key=lambda a: a.get("checked_at", "") or a.get("updated_at", ""),
                      reverse=True)
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
