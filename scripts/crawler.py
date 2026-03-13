#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 v5
- 每个站点多套解析策略，互为兜底
- 国家信息精确提取（不再默认美国）
- 速度优化：requests 优先，Selenium 按需
- 同邮箱保留最新密码，不丢弃
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

COUNTRY_RE = re.compile(
    r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|"
    r"俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|"
    r"荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古)"
)
COUNTRY_JS = "美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古"

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|"
    r"proton|pm|email|out1ok|live|msn)\.[a-z]{2,}\b",
    re.IGNORECASE)

EMAIL_ANY = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", re.IGNORECASE)

STATUS_BAD = {"异常", "不可用", "失效", "已失效", "暂无可用", "unavailable",
              "invalid", "error", "失效账号", "暂无", "locked"}

TIME_RE = re.compile(r"(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)")


# ══════════════════════════════════════════
# 基础工具
# ══════════════════════════════════════════

def uid(email):
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]


def bad(status):
    if not status:
        return False
    return any(k in status.lower().strip() for k in STATUS_BAD)


def now_cst():
    return datetime.now(CST).isoformat()


def decode_cfemail(encoded: str) -> str:
    """解码 Cloudflare data-cfemail 属性（XOR算法）"""
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""


def find_country(text: str) -> str:
    """从文本中提取国家，绝不默认美国"""
    if not text:
        return ""
    m = COUNTRY_RE.search(text)
    return m.group(1) if m else ""


def find_time(text: str) -> str:
    m = TIME_RE.search(text)
    return m.group(1).strip() if m else ""


def make_record(email, password, status="正常", checked_at="", country=""):
    return {
        "email": email.lower().strip(),
        "password": password.strip(),
        "status": status,
        "checked_at": checked_at,
        "country": country,
    }


def dedup(lst):
    seen, out = set(), []
    for r in lst:
        e = r.get("email", "").lower().strip()
        if e and e not in seen:
            seen.add(e)
            out.append(r)
    return out


# ══════════════════════════════════════════
# Selenium 工具
# ══════════════════════════════════════════

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


def scroll(driver, n=6):
    for _ in range(n):
        driver.execute_script("window.scrollBy(0,800);")
        time.sleep(0.4)


def close_popups(driver):
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
        "//button[text()='Ok']",
        "//button[text()='OK']",
        "//a[contains(.,'我知道了')]",
        "//div[contains(@class,'modal')]//button",
        "//div[contains(@class,'popup')]//button",
        "//*[@aria-label='Close']",
        "//*[contains(@class,'close-btn')]",
        "//*[contains(@class,'modal-close')]",
    ]
    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.4)
        except Exception:
            pass


def wait_for_content(driver, timeout=12):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: "@" in d.page_source and len(d.page_source) > 3000
        )
    except Exception:
        pass


# ══════════════════════════════════════════
# 解析策略库（3种独立策略）
# ══════════════════════════════════════════

def strategy_data_clipboard(html: str) -> list:
    """
    策略A：data-clipboard-text 按钮
    适用：139.196.183.52、idshare001.me、idfree.top、btvda、bocchi2b 等同类框架
    """
    soup = BeautifulSoup(html, "lxml")
    results = []

    cards = soup.select(".card-body")
    if not cards:
        seen_parents = []
        for btn in soup.select("[data-clipboard-text]"):
            p = btn.find_parent(class_=lambda c: c and any(
                k in c for k in ("col", "card", "item", "account")))
            if p and p not in seen_parents:
                seen_parents.append(p)
        cards = seen_parents if seen_parents else [soup]

    for card in cards:
        email = ""
        for sel in [".copy-btn", "[id^='username_']",
                    "button.btn-primary[data-clipboard-text]",
                    "a.btn-primary[data-clipboard-text]"]:
            btn = card.select_one(sel)
            if btn:
                v = btn.get("data-clipboard-text", "").strip().lower()
                if v and "@" in v:
                    email = v
                    break
        # 带@的 data-clipboard-text 按钮（兜底）
        if not email:
            for btn in card.select("[data-clipboard-text]"):
                v = btn.get("data-clipboard-text", "").strip().lower()
                if v and "@" in v:
                    email = v
                    break
        # Cloudflare 邮件保护
        if not email:
            cf = card.select_one(".__cf_email__")
            if cf:
                encoded = cf.get("data-cfemail", "")
                if encoded:
                    email = decode_cfemail(encoded).lower()
                elif cf.get("href", "").startswith("mailto:"):
                    email = cf["href"][7:].lower()
        if not email or "@" not in email:
            continue

        # 密码
        password = ""
        for sel in [".copy-pass-btn", "[id^='password_']",
                    "button.btn-success[data-clipboard-text]",
                    "a.btn-success[data-clipboard-text]"]:
            btn = card.select_one(sel)
            if btn:
                v = btn.get("data-clipboard-text", "").strip()
                if v and len(v) >= 4 and "@" not in v:
                    password = v
                    break
        # 非@的 data-clipboard-text 按钮（兜底）
        if not password:
            for btn in card.select("[data-clipboard-text]"):
                v = btn.get("data-clipboard-text", "").strip()
                if v and "@" not in v and 4 <= len(v) <= 64:
                    password = v
                    break
        if not password:
            continue

        card_text = card.get_text(" ", strip=True)
        badge = card.select_one(".badge")
        if badge and bad(badge.get_text(strip=True)):
            continue

        # 国家：先从父级容器找，再从卡片文本找
        country = ""
        for ancestor in card.parents:
            ct = ancestor.get_text(" ", strip=True)
            country = find_country(ct)
            if country:
                break
        if not country:
            country = find_country(card_text)

        results.append(make_record(
            email, password,
            checked_at=find_time(card_text),
            country=country
        ))
    return results


def strategy_mailto_onclick(html: str) -> list:
    """
    策略B：mailto href 邮箱 + onclick copy('密码')
    适用：ccbaohe.com、tkbaohe.com
    """
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
                    email = decode_cfemail(encoded).lower()
            if not email or "@" not in email:
                t = cf.get_text(strip=True).lower()
                if "@" in t:
                    email = t
        if not email or "@" not in email:
            # 尝试从 data-clipboard-text 找邮箱（兜底）
            for btn in card.select("[data-clipboard-text]"):
                v = btn.get("data-clipboard-text", "").strip().lower()
                if "@" in v:
                    email = v
                    break
        if not email or "@" not in email:
            continue

        # 密码：从 onclick copy() 提取
        password = ""
        for btn in card.select("button"):
            if "密码" not in btn.get_text(strip=True):
                continue
            oc = btn.get("onclick", "")
            # 格式1: copy('abc')
            m = re.search(r"copy\('([^']{4,64})'\)", oc)
            if not m:
                # 格式2: copy("abc")
                m = re.search(r'copy\("([^"]{4,64})"\)', oc)
            if not m:
                # 格式3: HTML实体 &#39;
                m = re.search(r"copy\(&#39;([^&]{4,64})&#39;\)", oc)
            if not m:
                # 格式4: 宽松匹配括号内容
                m = re.search(r"copy\(([A-Za-z0-9!@#$%^&*()\-_=+]{4,64})\)", oc)
            if m:
                password = m.group(1).strip()
                break
        # 兜底：data-clipboard-text
        if not password:
            for btn in card.select("[data-clipboard-text]"):
                v = btn.get("data-clipboard-text", "").strip()
                if v and "@" not in v and 4 <= len(v) <= 64:
                    password = v
                    break
        if not password or "@" in password or len(password) < 4:
            continue

        card_text = card.get_text(" ", strip=True)
        if re.search(r"(异常|失效|不可用|锁定|disabled)", card_text, re.I):
            continue

        # 国家：优先从 card-header 取
        country = ""
        header = card.find_previous("div", class_="card-header")
        if header:
            country = find_country(header.get_text())
        if not country:
            country = find_country(card_text)

        mt = re.search(r"检测时间[：:\s]*(20\d{2}-\d{2}-\d{2}\s\d{2}:\d{2}(?::\d{2})?)", card_text)
        checked_at = mt.group(1) if mt else find_time(card_text)

        results.append(make_record(email, password, checked_at=checked_at, country=country))
    return results


def strategy_plaintext(html: str) -> list:
    """
    策略C：纯文本格式
    适用：free.iosapp.icu（账号: xxx  密码: xxx  状态: xxx）
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)
    results = []
    seen = set()

    blocks = re.split(r"(?=账[号号][:：])", text, flags=re.IGNORECASE)
    if len(blocks) <= 1:
        # 尝试按编号分割（编号1 编号2...）
        blocks = re.split(r"(?=编号\s*\d+)", text)

    for block in blocks:
        me = re.search(
            r"账[号号][:：]\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})",
            block, re.I)
        if not me:
            me = EMAIL_ANY.search(block)
        if not me:
            continue
        email = (me.group(1) if me.lastindex else me.group(0)).lower().strip()
        if not email or "@" not in email or email in seen:
            continue

        mp = re.search(r"密码[:：]\s*(\S{4,64})", block)
        if not mp:
            continue
        password = mp.group(1).strip()
        if "@" in password:
            continue

        ms = re.search(r"状[态態][:：]\s*(\S+)", block)
        if ms and bad(ms.group(1)):
            continue

        mt = re.search(r"检查时间[:：]?\s*(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)", block)
        checked_at = mt.group(1).strip() if mt else find_time(block)

        seen.add(email)
        results.append(make_record(email, password,
                                   checked_at=checked_at,
                                   country=find_country(block)))
    return results


def strategy_js_scan(driver) -> list:
    """
    策略D：JS全量扫描（Selenium专用）
    多重方式提取密码，精确提取国家
    """
    try:
        raw = driver.execute_script("""
var results = [];
var seen = {};
var EMAIL_P = /[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i;
var COUNTRY_P = /(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|俄罗斯|巴西|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|荷兰|蒙古|中国大陆)/;
var TIME_P = /(20\\d{2}-\\d{2}-\\d{2}[\\sT]\\d{2}:\\d{2}(?::\\d{2})?)/;
var BAD_P = /异常|失效|不可用|锁定/;

var containers = Array.from(document.querySelectorAll(
    '.card-body,.card,.item,.id-item,.account-item'
));
if(containers.length < 2) {
    containers = Array.from(document.querySelectorAll('[class]')).filter(function(el) {
        var t = el.innerText || '';
        return EMAIL_P.test(t) && t.length < 2000 && t.length > 20;
    });
}

containers.forEach(function(card) {
    var text = (card.innerText || card.textContent || '').trim();
    if(!text || text.length > 3000) return;
    var emailMatch = text.match(EMAIL_P);
    if(!emailMatch) return;
    var email = emailMatch[0].toLowerCase();
    if(seen[email]) return;
    if(BAD_P.test(text)) return;

    var pwd = '';

    // 策略1: data-clipboard-text（不含@）
    var clipEls = card.querySelectorAll('[data-clipboard-text]');
    clipEls.forEach(function(el) {
        if(pwd) return;
        var v = (el.getAttribute('data-clipboard-text') || '').trim();
        if(v && v.indexOf('@') < 0 && v.length >= 4 && v.length <= 64) pwd = v;
    });

    // 策略2: onclick copy()
    if(!pwd) {
        card.querySelectorAll('button,a').forEach(function(btn) {
            if(pwd) return;
            var oc = btn.getAttribute('onclick') || '';
            var m = oc.match(/copy\\('([^']{4,64})'\\)/) ||
                    oc.match(/copy\\("([^"]{4,64})"\\)/) ||
                    oc.match(/copy\\(&#39;([^&]{4,64})&#39;\\)/);
            if(m && m[1].indexOf('@') < 0) pwd = m[1];
        });
    }

    // 策略3: input value
    if(!pwd) {
        card.querySelectorAll('input').forEach(function(inp) {
            if(pwd) return;
            var v = (inp.value || inp.getAttribute('value') || '').trim();
            if(v && v.indexOf('@') < 0 && v.length >= 4 && v.length <= 64) pwd = v;
        });
    }

    if(!pwd || pwd.length < 4) return;

    // 国家：从包含该卡片的祖先元素中找（更准确）
    var country = '';
    var el = card;
    for(var i=0; i<5; i++) {
        el = el.parentElement;
        if(!el) break;
        var ct = el.innerText || '';
        var cm = ct.match(COUNTRY_P);
        if(cm) { country = cm[1]; break; }
    }
    if(!country) {
        var cm2 = text.match(COUNTRY_P);
        if(cm2) country = cm2[1];
    }

    var tm = text.match(TIME_P);
    seen[email] = 1;
    results.push({
        email: email, pwd: pwd,
        time: tm ? tm[1].trim() : '',
        country: country
    });
});
return results;
        """)
        results = []
        seen = set()
        for d in (raw or []):
            e = (d.get("email") or "").lower().strip()
            p = (d.get("pwd") or "").strip()
            if e and p and "@" in e and 4 <= len(p) <= 64 and e not in seen:
                seen.add(e)
                results.append(make_record(e, p,
                                           checked_at=(d.get("time") or "").strip(),
                                           country=d.get("country") or ""))
        return results
    except Exception as ex:
        logger.debug(f"strategy_js_scan error: {ex}")
        return []


def try_all_static(html: str) -> list:
    """依次尝试3种静态策略，返回结果最多的"""
    ra = strategy_data_clipboard(html)
    rb = strategy_mailto_onclick(html)
    rc = strategy_plaintext(html)
    best = max([ra, rb, rc], key=len)
    return best


def fetch_html(url: str, timeout: int = 12) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.encoding = "utf-8"
        return resp.text if resp.status_code == 200 else ""
    except Exception:
        return ""


def crawl_url(driver, url: str, wait: int = 6, scroll_n: int = 8) -> list:
    """通用入口：requests → Selenium，静态解析 → JS扫描"""
    # 1. requests + 静态
    html = fetch_html(url)
    if html and "@" in html:
        r = try_all_static(html)
        if r:
            logger.info(f"    [requests] {url} → {len(r)} 条")
            return r

    # 2. Selenium + 静态
    try:
        driver.get(url)
        time.sleep(wait)
        close_popups(driver)
        scroll(driver, n=scroll_n)
        time.sleep(1)
        r = try_all_static(driver.page_source)
        if r:
            logger.info(f"    [selenium+static] {url} → {len(r)} 条")
            return r
        # 3. JS扫描
        r = strategy_js_scan(driver)
        if r:
            logger.info(f"    [selenium+js] {url} → {len(r)} 条")
        return r
    except Exception as ex:
        logger.error(f"    crawl_url error {url}: {ex}")
        return []


# ══════════════════════════════════════════
# 各站点专属爬虫
# ══════════════════════════════════════════

def crawl_idshare001(driver) -> list:
    """idshare001.me — 不稳定，多路径+多策略"""
    urls = [
        "https://idshare001.me/goso.html",
        "https://idshare001.me/",
        "https://idshare001.me/free",
        "https://idshare001.me/share",
    ]
    # requests 逐个试
    for url in urls:
        html = fetch_html(url)
        if html and "@" in html and len(html) > 3000:
            r = try_all_static(html)
            if r:
                logger.info(f"  idshare001 [requests] {url} → {len(r)} 条")
                return dedup(r)

    # Selenium 逐个试
    for url in urls:
        try:
            driver.get(url)
            time.sleep(5)
            close_popups(driver)
            wait_for_content(driver, timeout=10)
            scroll(driver, n=6)
            html = driver.page_source
            if "@" not in html or len(html) < 3000:
                continue
            r = try_all_static(html)
            if not r:
                r = strategy_js_scan(driver)
            if r:
                logger.info(f"  idshare001 [selenium] {url} → {len(r)} 条")
                return dedup(r)
        except Exception:
            continue

    logger.info("  idshare001 抓到: 0")
    return []


def crawl_idfree_top(driver) -> list:
    """idfree.top"""
    for url in ["https://idfree.top/", "https://www.idfree.top/"]:
        r = crawl_url(driver, url, wait=8)
        if r:
            logger.info(f"  idfree_top 抓到: {len(r)}")
            return dedup(r)
    logger.info("  idfree_top 抓到: 0")
    return []


def crawl_ip_share(driver) -> list:
    """139.196.183.52 — data-clipboard-text + Cloudflare 邮件保护"""
    base = "http://139.196.183.52"
    discovered = {f"{base}/share/DZhBvnglEU"}

    # 从首页发现更多链接
    for idx in [f"{base}/", f"{base}/share"]:
        html = fetch_html(idx)
        if html:
            for m in re.finditer(r'href=["\']([^"\']*?/share/[^"\']+)["\']', html):
                href = m.group(1)
                full = href if href.startswith("http") else f"{base}{href}"
                discovered.add(full)

    all_results = []
    for url in discovered:
        html = fetch_html(url)
        if html:
            r = strategy_data_clipboard(html)
            if r:
                logger.info(f"    [requests] {url} → {len(r)} 条")
                all_results.extend(r)
                continue
        r = crawl_url(driver, url, wait=5, scroll_n=5)
        all_results.extend(r)

    logger.info(f"  139.196.183.52 抓到: {len(dedup(all_results))}")
    return dedup(all_results)


def crawl_free_iosapp_icu(driver) -> list:
    """free.iosapp.icu — 纯文本格式"""
    url = "https://free.iosapp.icu/"
    html = fetch_html(url)
    if html:
        r = strategy_plaintext(html)
        if r:
            logger.info(f"  free.iosapp.icu [requests] → {len(r)} 条")
            return dedup(r)
    try:
        driver.get(url)
        time.sleep(5)
        close_popups(driver)
        scroll(driver, n=6)
        r = strategy_plaintext(driver.page_source)
        if not r:
            r = strategy_js_scan(driver)
        logger.info(f"  free.iosapp.icu [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  free.iosapp.icu error: {ex}")
        return []


def crawl_app_iosr_cn(driver) -> list:
    """app.iosr.cn — JS动态渲染，必须Selenium，有刷新按钮"""
    try:
        driver.get("https://app.iosr.cn/tools/apple-shared-id")
        time.sleep(6)
        close_popups(driver)
        for xpath in ["//button[contains(.,'刷新')]", "//button[contains(.,'Refresh')]"]:
            try:
                btn = driver.find_element(By.XPATH, xpath)
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(3)
                break
            except Exception:
                pass
        scroll(driver, n=8)
        time.sleep(2)
        r = try_all_static(driver.page_source)
        if not r:
            r = strategy_js_scan(driver)
        logger.info(f"  app.iosr.cn 抓到: {len(r)}")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  app.iosr.cn error: {ex}")
        return []


def crawl_shadowrocket_best(driver) -> list:
    """shadowrocket.best — 无限滚动"""
    url = "https://shadowrocket.best/"
    html = fetch_html(url)
    if html and "@" in html:
        r = try_all_static(html)
        if r:
            logger.info(f"  shadowrocket.best [requests] → {len(r)} 条")
            return dedup(r)
    try:
        driver.get(url)
        time.sleep(6)
        close_popups(driver)
        last_count = 0
        for _ in range(30):
            driver.execute_script("window.scrollBy(0, 600);")
            time.sleep(0.4)
            cards = driver.find_elements(By.CSS_SELECTOR, ".card,.id-card,[class*='card']")
            if len(cards) == last_count:
                break
            last_count = len(cards)
        driver.execute_script("window.scrollTo(0,0)")
        time.sleep(1)
        r = try_all_static(driver.page_source)
        if not r:
            r = strategy_js_scan(driver)
        logger.info(f"  shadowrocket.best [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  shadowrocket.best error: {ex}")
        return []


def crawl_ccbaohe(driver) -> list:
    """ccbaohe.com — mailto邮箱 + onclick copy('密码')"""
    url = "https://ccbaohe.com/appleID/"
    html = fetch_html(url)
    if html and "@" in html:
        r = strategy_mailto_onclick(html)
        if r:
            logger.info(f"  ccbaohe [requests] → {len(r)} 条")
            return dedup(r)
    try:
        driver.get(url)
        time.sleep(8)
        close_popups(driver)
        scroll(driver, n=10)
        time.sleep(2)
        r = strategy_mailto_onclick(driver.page_source)
        if not r:
            r = strategy_js_scan(driver)
        logger.info(f"  ccbaohe [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  ccbaohe error: {ex}")
        return []


def crawl_tkbaohe(driver) -> list:
    """tkbaohe.com — 与ccbaohe相同结构"""
    url = "https://tkbaohe.com/Shadowrocket/"
    html = fetch_html(url)
    if html and "@" in html:
        r = strategy_mailto_onclick(html)
        if r:
            logger.info(f"  tkbaohe [requests] → {len(r)} 条")
            return dedup(r)
    try:
        driver.get(url)
        time.sleep(8)
        close_popups(driver)
        scroll(driver, n=10)
        time.sleep(2)
        r = strategy_mailto_onclick(driver.page_source)
        if not r:
            r = strategy_js_scan(driver)
        logger.info(f"  tkbaohe [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  tkbaohe error: {ex}")
        return []


def crawl_id_btvda_top(driver) -> list:
    """id.btvda.top — 账号最多，data-clipboard-text"""
    url = "https://id.btvda.top/"
    r = crawl_url(driver, url, wait=6, scroll_n=15)
    logger.info(f"  id.btvda.top 抓到: {len(r)}")
    return dedup(r)


def crawl_bocchi2b(driver) -> list:
    """id.bocchi2b.top — data-clipboard-text，有弹窗"""
    url = "https://id.bocchi2b.top/"
    html = fetch_html(url)
    if html and "@" in html:
        r = try_all_static(html)
        if r:
            logger.info(f"  bocchi2b [requests] → {len(r)} 条")
            return dedup(r)
    try:
        driver.get(url)
        time.sleep(6)
        for _ in range(4):
            close_popups(driver)
            time.sleep(0.4)
        wait_for_content(driver, timeout=15)
        scroll(driver, n=12)
        time.sleep(2)
        r = try_all_static(driver.page_source)
        if not r:
            r = strategy_js_scan(driver)
        logger.info(f"  bocchi2b [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  bocchi2b error: {ex}")
        return []


# ══════════════════════════════════════════
# 站点配置（按指定顺序）
# ══════════════════════════════════════════

SITES = [
    {"name": "idshare001.me",       "fn": crawl_idshare001},
    {"name": "idfree.top",          "fn": crawl_idfree_top},
    {"name": "139.196.183.52",      "fn": crawl_ip_share},
    {"name": "free.iosapp.icu",     "fn": crawl_free_iosapp_icu},
    {"name": "app.iosr.cn",         "fn": crawl_app_iosr_cn},
    {"name": "shadowrocket.best",   "fn": crawl_shadowrocket_best},
    {"name": "ccbaohe.com/appleID", "fn": crawl_ccbaohe},
    {"name": "tkbaohe.com",         "fn": crawl_tkbaohe},
    {"name": "id.btvda.top",        "fn": crawl_id_btvda_top},
    {"name": "id.bocchi2b.top",     "fn": crawl_bocchi2b},
]

SITE_ORDER = {s["name"]: i for i, s in enumerate(SITES)}


# ══════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════

def crawl_all():
    records = {}       # email → 账号记录
    source_stats = {}

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
                if not e or not pw or "@" not in e:
                    continue
                if len(pw) < 4 or len(pw) > 64:
                    continue
                if len(set(pw)) < 2:
                    continue

                if e not in records:
                    records[e] = {
                        "id":         uid(e),
                        "email":      e,
                        "password":   pw,
                        "status":     p.get("status", "正常"),
                        "country":    p.get("country", ""),
                        "checked_at": p.get("checked_at", ""),
                        "source":     site["name"],
                        "updated_at": now_cst(),
                    }
                    nc += 1
                else:
                    # 同邮箱：用更新的数据覆盖密码和时间
                    existing = records[e]
                    new_t = p.get("checked_at", "")
                    old_t = existing.get("checked_at", "")
                    if new_t and new_t > old_t:
                        existing["password"] = pw
                        existing["checked_at"] = new_t
                    if p.get("country") and not existing.get("country"):
                        existing["country"] = p["country"]

            source_stats[site["name"]] = nc
            logger.info(f"  → 新增 {nc} 条（共 {len(records)} 条）[{site['name']} 抓到 {len(pairs)} 条]")
            time.sleep(1)
    finally:
        driver.quit()
        logger.info("浏览器已关闭")

    # 排序：先按站点顺序，同站点内按检查时间升序
    def sort_key(a):
        site_rank = SITE_ORDER.get(a.get("source", ""), 999)
        t = a.get("checked_at", "") or a.get("updated_at", "") or ""
        return (site_rank, t)

    accounts = sorted(records.values(), key=sort_key)

    return {
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M"),
        "total":        len(accounts),
        "source_stats": source_stats,
        "accounts":     accounts,
    }


if __name__ == "__main__":
    output_path = os.environ.get("OUTPUT_FILE", "apple_ids.json")
    result = crawl_all()
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 完成！共输出 {result['total']} 条账号 → {output_path}")
