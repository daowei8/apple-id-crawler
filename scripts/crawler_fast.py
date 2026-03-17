#!/usr/bin/env python3
"""
Apple ID 快速爬虫 — crawler_fast.py
负责高频更新站点（每 1 分钟爬一次）：
  1. idshare001.me       — 直接请求 /node/getid.php?getid=1 和 getid=2（纯 requests）
  2. ios.juzixp.com      — 纯 requests 解析 onclick 中的账密
  3. applexp 美区        — 直接请求 pga.juzixp.top API
  4. applexp 日区        — 直接请求 pga.juzixp.top API
  5. applexp 港区        — 直接请求 pga.juzixp.top API
  6. applexp 小火箭      — 直接请求 pga.juzixp.top + dc.juzixp.top API

新增的 2-6 全部是纯 requests，不需要 Selenium，并发执行，几乎不增加时间。
结果合并写入 apple_ids.json（与 crawler_slow.py / crawler_mid.py 共用同一文件）
合并策略：保留现有 slow/mid 站点账号，用本次新数据覆盖 fast 站点账号。
"""

import re, json, time, hashlib, logging, os
import concurrent.futures
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ── logging ────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class _CSTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        cst = timezone(timedelta(hours=8))
        ct = datetime.fromtimestamp(record.created, tz=cst)
        return ct.strftime('%Y-%m-%d %H:%M:%S')
for _h in logging.root.handlers:
    _h.setFormatter(_CSTFormatter('%(asctime)s [%(levelname)s] %(message)s'))

CST = timezone(timedelta(hours=8))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

VALID_DOMAINS = {
    "icloud.com", "me.com", "mac.com",
    "gmail.com",
    "outlook.com", "hotmail.com", "live.com", "msn.com",
    "qq.com", "163.com", "126.com",
    "yahoo.com", "yahoo.co.jp",
    "protonmail.com", "proton.me",
    "email.com",
    "kuaiyun1.com",
    "eml.ru", "cnap.biz",
}

COUNTRY_RE = re.compile(
    r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|"
    r"俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|"
    r"荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古)"
)

STATUS_BAD = {"异常", "不可用", "失效", "已失效", "locked", "invalid"}

FAST_SOURCES = {
    "idshare001.me",
    "ios.juzixp.com",
    "applexp/美区",
    "applexp/日区",
    "applexp/港区",
    "applexp/小火箭",
}

SITE_ORDER = [
    "idfree.top", "idshare001.me",
    "ios.juzixp.com",
    "applexp/美区", "applexp/日区", "applexp/港区", "applexp/小火箭",
    "ccbaohe.com/appleID", "tkbaohe.com",
    "id.btvda.top", "id.bocchi2b.top",
    "fx.xdd.net.tr",
]


# ══════════════════════════════════════════
# 基础工具
# ══════════════════════════════════════════

def is_valid_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    parts = email.lower().split("@")
    if len(parts) != 2:
        return False
    local, domain = parts
    if len(local) < 2:
        return False
    return domain in VALID_DOMAINS


def uid(email):
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]


def bad(status):
    return any(k in (status or "").lower() for k in STATUS_BAD)


def now_cst():
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def find_country(text: str) -> str:
    m = COUNTRY_RE.search(text or "")
    return m.group(1) if m else ""


def dedup(lst):
    seen, out = set(), []
    for r in lst:
        e = r.get("email", "").lower().strip()
        if e and e not in seen and is_valid_email(e):
            seen.add(e)
            out.append(r)
    return out


def fetch_html(url: str, timeout: int = 12) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.encoding = "utf-8"
        return resp.text if resp.status_code == 200 else ""
    except Exception:
        return ""


def fetch_json(url: str, timeout: int = 12):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception as ex:
        logger.debug(f"  fetch_json {url} 失败: {ex}")
    return None


# ══════════════════════════════════════════
# Selenium 工具（仅 idshare001 兜底使用）
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
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
    return driver


def scroll(driver, n=8):
    for _ in range(n):
        driver.execute_script("window.scrollBy(0,700);")
        time.sleep(0.5)


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
        "//a[contains(.,'我知道了')]",
        "//div[contains(@class,'modal')]//button",
        "//div[contains(@class,'dialog')]//button",
        "//div[contains(@class,'popup')]//button",
        "//*[@aria-label='Close']",
        "//*[contains(@class,'close-btn')]",
    ]
    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.5)
        except Exception:
            pass


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

INTERCEPT_JS = r"""
window.__api_responses = window.__api_responses || [];
window.__api_all = window.__api_all || [];
const _origFetch = window.fetch;
window.fetch = function() {
    var args = arguments;
    return _origFetch.apply(this, args).then(function(resp) {
        try {
            var url = (args[0] && args[0].url) || args[0] || '';
            resp.clone().json().then(function(data) {
                window.__api_all.push({url: String(url), data: data});
            }).catch(function(){});
        } catch(e) {}
        return resp;
    });
};
const _origOpen = XMLHttpRequest.prototype.open;
const _origSend = XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.open = function(method, url) {
    this.__url = url;
    return _origOpen.apply(this, arguments);
};
XMLHttpRequest.prototype.send = function() {
    var self = this;
    this.addEventListener('load', function() {
        try {
            var data = JSON.parse(self.responseText);
            window.__api_all.push({url: String(self.__url||''), data: data});
        } catch(e) {}
    });
    return _origSend.apply(this, arguments);
};
"""


def extract_from_vue_api(driver, wait_secs=15, site_name="") -> list:
    driver.execute_script(INTERCEPT_JS)
    deadline = time.time() + wait_secs
    while time.time() < deadline:
        time.sleep(0.5)
        all_calls = driver.execute_script("return window.__api_all || []")
        for call in all_calls:
            data = call.get("data")
            if isinstance(data, list) and len(data) > 0:
                first = data[0]
                if isinstance(first, dict) and any(isinstance(v, str) for v in first.values()):
                    return data
            if isinstance(data, dict):
                accounts = data.get("id") or data.get("accounts") or []
                if isinstance(accounts, list) and len(accounts) > 0:
                    first = accounts[0]
                    if isinstance(first, dict) and (first.get("email") or first.get("account")):
                        return accounts
    logger.info(f"  {site_name} API拦截超时")
    return []


# ══════════════════════════════════════════
# 解析工具
# ══════════════════════════════════════════

def parse_vue_accounts(raw_list: list, site_name="") -> list:
    results = []
    if not raw_list:
        return results
    first = raw_list[0]
    logger.info(f"  {site_name} 样本字段: {list(first.keys()) if isinstance(first, dict) else type(first)}")
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        email = str(item.get("email") or item.get("username") or
                    item.get("account") or item.get("user") or "").strip().lower()
        pw = str(item.get("password") or item.get("pwd") or
                 item.get("pass") or item.get("passwd") or "").strip()
        try:
            if "\\u" in pw or "%u" in pw:
                pw = pw.encode("raw_unicode_escape").decode("unicode_escape")
        except Exception:
            pass
        raw_status = item.get("status", 1)
        status_ok = (raw_status == 1) if isinstance(raw_status, int) else not bad(str(raw_status))
        raw_country = str(item.get("country") or item.get("region") or item.get("area") or "")
        country = find_country(raw_country) or "美国"
        if not email or "@" not in email or not pw:
            continue
        if not is_valid_email(email) or not status_ok:
            continue
        results.append({
            "email": email, "password": pw, "status": "正常",
            "checked_at": now_cst(), "country": country,
        })
    return results


def strategy_data_clipboard(html: str) -> list:
    """idfree.top 专用：button[id^=username_N] + button[id^=password_N] 精确配对"""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen = set()

    for btn in soup.select("button[id^='username_'], a[id^='username_']"):
        n = btn.get("id", "")[9:]
        email = btn.get("data-clipboard-text", "").strip().lower()
        if not is_valid_email(email) or email in seen:
            continue
        pw_btn = soup.select_one(f"#password_{n}")
        if not pw_btn:
            continue
        pw = pw_btn.get("data-clipboard-text", "").strip()
        if not pw or "@" in pw or len(pw) < 4:
            continue
        card = btn.find_parent(class_="card-body") or btn.find_parent(class_="card")
        country = ""
        if card:
            for anc in card.parents:
                country = find_country(anc.get_text(" ", strip=True)[:300])
                if country:
                    break
        seen.add(email)
        results.append({"email": email, "password": pw, "status": "正常",
                         "checked_at": now_cst(), "country": country})
    if results:
        return results

    for card in soup.select(".card-body"):
        email = ""
        for sel in [".copy-btn", "button.btn-primary[data-clipboard-text]"]:
            b = card.select_one(sel)
            if b:
                v = b.get("data-clipboard-text", "").strip().lower()
                if is_valid_email(v):
                    email = v
                    break
        if not email or email in seen:
            continue
        pw = ""
        for sel in [".copy-pass-btn", "button.btn-success[data-clipboard-text]"]:
            b = card.select_one(sel)
            if b:
                v = b.get("data-clipboard-text", "").strip()
                if v and "@" not in v and 4 <= len(v) <= 64:
                    pw = v
                    break
        if not pw:
            continue
        country = ""
        for anc in card.parents:
            country = find_country(anc.get_text(" ", strip=True)[:300])
            if country:
                break
        seen.add(email)
        results.append({"email": email, "password": pw, "status": "正常",
                         "checked_at": now_cst(), "country": country})
    return results


def click_card_by_card(driver, account_cls, password_cls):
    driver.execute_script(HOOK_JS)
    time.sleep(0.3)
    results = []
    seen = set()
    cards = driver.find_elements(By.CSS_SELECTOR, ".card-body, .card")
    for card in cards:
        try:
            acct_btns = card.find_elements(By.CSS_SELECTOR, account_cls)
            if not acct_btns:
                continue
            before1 = driver.execute_script("return window.__copied.length;")
            driver.execute_script("arguments[0].click();", acct_btns[0])
            time.sleep(0.15)
            copied1 = driver.execute_script("return window.__copied||[]")
            if len(copied1) <= before1:
                continue
            email_val = copied1[-1].strip().lower()
            if not is_valid_email(email_val) or email_val in seen:
                continue
            pw_btns = card.find_elements(By.CSS_SELECTOR, password_cls)
            if not pw_btns:
                continue
            before2 = driver.execute_script("return window.__copied.length;")
            driver.execute_script("arguments[0].click();", pw_btns[0])
            time.sleep(0.15)
            copied2 = driver.execute_script("return window.__copied||[]")
            if len(copied2) <= before2:
                continue
            pw_val = copied2[-1].strip()
            if not pw_val or "@" in pw_val or len(pw_val) < 4:
                continue
            seen.add(email_val)
            results.append({"email": email_val, "password": pw_val,
                             "status": "正常", "checked_at": now_cst(), "country": ""})
        except Exception:
            continue
    return results


# ══════════════════════════════════════════
# 站点爬虫
# ══════════════════════════════════════════

def crawl_idshare001(driver) -> list:
    raw = []
    for api_path in ["/node/getid.php?getid=2", "/node/getid.php?getid=1"]:
        try:
            resp = requests.get("https://idshare001.me" + api_path,
                                headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    raw.extend(data)
                    logger.info(f"  idshare001 API {api_path} → {len(data)} 条")
        except Exception as ex:
            logger.debug(f"  idshare001 API {api_path} 失败: {ex}")

    if not raw:
        logger.info("  idshare001 直接 API 无数据，启动 Selenium 兜底…")
        for url in ["https://idshare001.me/goso.html", "https://idshare001.me/"]:
            try:
                driver.get("about:blank")
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
                    {"source": INTERCEPT_JS})
                driver.get(url)
                WebDriverWait(driver, 12).until(
                    lambda d: d.execute_script("return document.readyState") == "complete")
                if len(driver.page_source) > 2000:
                    break
            except Exception:
                continue
        for xpath in ["//button[contains(.,'我是老玩家')]", "//button[contains(.,'老玩家')]"]:
            try:
                btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath)))
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(1)
                break
            except Exception:
                pass
        raw = extract_from_vue_api(driver, wait_secs=15, site_name="idshare001")

    results = parse_vue_accounts(raw, "idshare001")
    logger.info(f"  idshare001 最终: {len(results)} 条")
    return dedup(results)


def crawl_idfree_top(driver) -> list:
    html = fetch_html("https://idfree.top/")
    if html and "@" in html:
        r = strategy_data_clipboard(html)
        if r:
            logger.info(f"  idfree.top [requests] → {len(r)} 条")
            return dedup(r)

    loaded = False
    for url in ["https://idfree.top/", "https://www.idfree.top/"]:
        try:
            driver.get(url)
            WebDriverWait(driver, 12).until(
                lambda d: d.execute_script("return document.readyState") == "complete")
            if len(driver.page_source) > 2000:
                loaded = True
                break
        except Exception:
            continue

    if not loaded:
        logger.info("  idfree.top 加载失败")
        return []

    time.sleep(2)
    for xpath in [
        "//button[contains(.,'我已阅读')]",
        "//button[contains(.,'继续查看账号')]",
        "//button[contains(.,'继续查看')]",
        "//button[contains(.,'查看账号')]",
    ]:
        try:
            btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", btn)
            logger.info(f"  idfree 点击: {btn.text.strip()}")
            time.sleep(2)
            break
        except Exception:
            pass
    close_popups(driver)
    scroll(driver, n=10)
    time.sleep(2)

    results = strategy_data_clipboard(driver.page_source)
    if not results:
        results = click_card_by_card(driver, ".btn-copy-account", ".btn-copy-password")
    if not results:
        driver.execute_script(HOOK_JS)
        time.sleep(0.3)
        xpath_btns = (
            "//button[contains(.,'复制账号') or contains(.,'账号')]"
            " | //button[contains(.,'复制密码') or contains(.,'密码')]"
            " | //button[contains(.,'复制') and not(contains(.,'链接'))]"
        )
        btns = driver.find_elements(By.XPATH, xpath_btns)
        emails_list, pwds_list = [], []
        for btn in btns[:300]:
            try:
                before = len(driver.execute_script("return window.__copied||[]"))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.12)
                after = driver.execute_script("return window.__copied||[]")
                if len(after) > before:
                    val = after[-1].strip()
                    if "@" in val:
                        emails_list.append(val.lower())
                    elif len(val) >= 5:
                        pwds_list.append(val)
            except Exception:
                pass
        seen = set()
        for i in range(min(len(emails_list), len(pwds_list))):
            e, p = emails_list[i], pwds_list[i]
            if is_valid_email(e) and p and e not in seen and len(p) >= 5:
                seen.add(e)
                results.append({"email": e, "password": p, "status": "正常",
                                 "checked_at": now_cst(), "country": ""})

    logger.info(f"  idfree.top 最终: {len(results)} 条")
    return dedup(results)


# ── 新增站点 1：ios.juzixp.com ──────────────────────────────

def crawl_ios_juzixp() -> list:
    url = "https://ios.juzixp.com/"
    html = fetch_html(url, timeout=15)
    if not html:
        logger.warning("  ios.juzixp.com 页面拉取失败")
        return []

    soup = BeautifulSoup(html, "lxml")
    results = []
    seen = set()

    for card in soup.select(".account-card"):
        btn_group = card.select_one(".btn-group")
        if not btn_group:
            continue

        email_btn = btn_group.select_one("button.copy-email")
        pwd_btn   = btn_group.select_one("button.copy-password")

        if not email_btn or not pwd_btn:
            continue

        def extract_copy_val(btn):
            oc = btn.get("onclick", "")
            m = re.search(r"handleCopy\('([^']+)'", oc)
            if not m:
                m = re.search(r'handleCopy\("([^"]+)"', oc)
            return m.group(1).strip() if m else ""

        email = extract_copy_val(email_btn).lower()
        pw    = extract_copy_val(pwd_btn)

        if not email or not pw:
            continue
        if not is_valid_email(email):
            continue
        if "@" in pw or len(pw) < 4 or len(pw) > 64:
            continue
        if email in seen:
            continue

        status_span = card.select_one(".status")
        status_text = status_span.get_text(strip=True) if status_span else "正常"
        if bad(status_text):
            continue

        country = ""
        for info_item in card.select(".info-item"):
            label = info_item.select_one(".info-label")
            if label and "国家" in label.get_text():
                spans = info_item.find_all("span")
                if len(spans) >= 2:
                    country = find_country(spans[-1].get_text(strip=True))
                break
        if not country:
            country = find_country(card.get_text(" ", strip=True))

        checked_at = now_cst()
        for info_item in card.select(".info-item"):
            label = info_item.select_one(".info-label")
            if label and "更新时间" in label.get_text():
                spans = info_item.find_all("span")
                if len(spans) >= 2:
                    t = spans[-1].get_text(strip=True)
                    if re.match(r"20\d{2}-\d{2}-\d{2}", t):
                        checked_at = t
                break

        seen.add(email)
        results.append({
            "email": email, "password": pw,
            "status": "正常", "checked_at": checked_at,
            "country": country,
        })

    logger.info(f"  ios.juzixp.com 最终: {len(results)} 条")
    return dedup(results)


# ── 新增站点 2-5：applexp 系列 ──────────────────────────────

def _parse_applexp_api_response(data, site_name: str) -> list:
    if not isinstance(data, dict):
        logger.warning(f"  {site_name} API 返回格式异常: {type(data)}")
        return []
    if data.get("code") != 200:
        logger.warning(f"  {site_name} API code={data.get('code')}, msg={data.get('msg')}")
        return []

    raw_list = data.get("data", [])
    if not isinstance(raw_list, list):
        logger.warning(f"  {site_name} data 字段不是列表")
        return []

    results = []
    seen = set()
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        email = str(item.get("email") or "").strip().lower()
        pw    = str(item.get("password") or "").strip()

        if not email or not pw:
            continue
        if not is_valid_email(email):
            continue
        if "@" in pw or len(pw) < 4 or len(pw) > 64:
            continue

        raw_status = item.get("status", 1)
        if isinstance(raw_status, int):
            if raw_status != 1:
                continue
        else:
            if bad(str(raw_status)):
                continue

        raw_country = str(item.get("country") or "")
        country = find_country(raw_country) or raw_country or "美国"

        updated = str(item.get("updatedTime") or item.get("createdTime") or "")
        checked_at = now_cst()
        m = re.match(r"(20\d{2}-\d{2}-\d{2})T(\d{2}:\d{2})", updated)
        if m:
            checked_at = f"{m.group(1)} {m.group(2)}:00"

        if email in seen:
            continue
        seen.add(email)
        results.append({
            "email": email, "password": pw,
            "status": "正常", "checked_at": checked_at,
            "country": country,
        })

    return results


def _parse_dc_juzixp_txt(text: str) -> dict:
    if not text or not text.strip():
        return {}
    result = {}
    lines = text.strip().splitlines()
    for line in lines:
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip()
        if key in ("账号", "邮箱"):
            result["email"] = val.lower()
        elif key == "密码":
            result["password"] = val
        elif key in ("类型", "国家"):
            result["country"] = find_country(val) or val
        elif key in ("检查时间", "上次检查"):
            result["checked_at"] = val
        elif key == "状态":
            result["status"] = val
    return result


def crawl_applexp_us() -> list:
    url = "https://pga.juzixp.top/api/AppleidShare/GetAll?Country=美国"
    data = fetch_json(url)
    if data is None:
        logger.warning("  applexp/美区 API 请求失败")
        return []
    results = _parse_applexp_api_response(data, "applexp/美区")
    for r in results:
        if not r.get("country"):
            r["country"] = "美国"
    logger.info(f"  applexp/美区 最终: {len(results)} 条")
    return dedup(results)


def crawl_applexp_jp() -> list:
    url = "https://pga.juzixp.top/api/AppleidShare/GetAll?Country=日本"
    data = fetch_json(url)
    if data is None:
        logger.warning("  applexp/日区 API 请求失败")
        return []
    results = _parse_applexp_api_response(data, "applexp/日区")
    for r in results:
        if not r.get("country"):
            r["country"] = "日本"
    logger.info(f"  applexp/日区 最终: {len(results)} 条")
    return dedup(results)


def crawl_applexp_hk() -> list:
    url = "https://pga.juzixp.top/api/AppleidShare/GetAll?Country=香港"
    data = fetch_json(url)
    if data is None:
        logger.warning("  applexp/港区 API 请求失败")
        return []
    results = _parse_applexp_api_response(data, "applexp/港区")
    for r in results:
        if not r.get("country"):
            r["country"] = "香港"
    logger.info(f"  applexp/港区 最终: {len(results)} 条")
    return dedup(results)


def crawl_applexp_shadowrocket() -> list:
    results = []
    seen_emails = set()

    url_api = "https://pga.juzixp.top/api/AppleidShare/GetAll?IsSck=1"
    data = fetch_json(url_api)
    if data is not None:
        api_results = _parse_applexp_api_response(data, "applexp/小火箭-API")
        for r in api_results:
            e = r["email"]
            if e not in seen_emails:
                seen_emails.add(e)
                results.append(r)
        logger.info(f"  applexp/小火箭 API → {len(api_results)} 条")
    else:
        logger.warning("  applexp/小火箭 主API 请求失败")

    for n in range(3):
        txt_url = f"https://dc.juzixp.top/go-rod/{n}.txt"
        try:
            resp = requests.get(txt_url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                continue
            resp.encoding = "utf-8"
            record = _parse_dc_juzixp_txt(resp.text)
            if not record:
                continue
            email = record.get("email", "")
            pw    = record.get("password", "")
            if not email or not pw:
                continue
            if not is_valid_email(email):
                continue
            if "@" in pw or len(pw) < 4 or len(pw) > 64:
                continue
            if bad(record.get("status", "正常")):
                continue
            if email in seen_emails:
                continue
            seen_emails.add(email)
            results.append({
                "email": email, "password": pw,
                "status": "正常",
                "checked_at": record.get("checked_at", now_cst()),
                "country": record.get("country", "美国"),
            })
            logger.info(f"  applexp/小火箭 {n}.txt → {email}")
        except Exception as ex:
            logger.debug(f"  applexp/小火箭 {n}.txt 失败: {ex}")

    logger.info(f"  applexp/小火箭 最终: {len(results)} 条")
    return dedup(results)


# ══════════════════════════════════════════
# 合并写入 apple_ids.json
# ══════════════════════════════════════════

def merge_and_save(fast_records: dict, output_path: str) -> dict:
    existing_accounts = []
    if Path(output_path).exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                old = json.load(f)
            existing_accounts = [
                a for a in old.get("accounts", [])
                if a.get("source") not in FAST_SOURCES
            ]
        except Exception as ex:
            logger.warning(f"读取现有文件失败: {ex}")

    merged = {a["email"]: a for a in existing_accounts}
    for e, rec in fast_records.items():
        merged[e] = rec

    order_map = {s: i for i, s in enumerate(SITE_ORDER)}
    accounts = sorted(
        merged.values(),
        key=lambda a: (order_map.get(a.get("source", ""), 999),
                       -(len(a.get("checked_at") or "")),
                       a.get("checked_at", "") or "")
    )

    # 每个来源内部按 checked_at 降序（最新的在前）
    from itertools import groupby
    groups = {}
    for a in merged.values():
        src = a.get("source", "unknown")
        groups.setdefault(src, []).append(a)
    for src in groups:
        groups[src].sort(key=lambda a: a.get("checked_at", "") or "", reverse=True)

    accounts = []
    for src in SITE_ORDER:
        accounts.extend(groups.get(src, []))
    for src, lst in groups.items():
        if src not in SITE_ORDER:
            accounts.extend(lst)

    source_stats = {}
    for a in accounts:
        src = a.get("source", "unknown")
        source_stats[src] = source_stats.get(src, 0) + 1

    result = {
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M"),
        "total": len(accounts),
        "source_stats": source_stats,
        "accounts": accounts,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 写入 {output_path}（共 {len(accounts)} 条）")
    return result


# ══════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════

def _make_record(p: dict, source: str) -> tuple:
    e  = p.get("email", "").strip().lower()
    pw = p.get("password", "").strip()
    if not is_valid_email(e) or not pw or len(pw) < 4 or len(pw) > 64:
        return None, None
    if len(set(pw)) < 2:
        return None, None
    if "&amp;" in pw:
        pw = pw.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return e, {
        "id": uid(e), "email": e, "password": pw,
        "status": p.get("status", "正常"),
        "country": p.get("country", ""),
        "checked_at": p.get("checked_at", now_cst()),
        "source": source,
        "updated_at": now_cst(),
    }


def crawl_fast():
    records = {}
    source_stats = {}

    logger.info("【快速爬虫】第一阶段：并发请求纯 requests 站点…")

    pure_tasks = [
        (crawl_ios_juzixp,            "ios.juzixp.com"),
        (crawl_applexp_us,            "applexp/美区"),
        (crawl_applexp_jp,            "applexp/日区"),
        (crawl_applexp_hk,            "applexp/港区"),
        (crawl_applexp_shadowrocket,  "applexp/小火箭"),
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_source = {
            executor.submit(fn): src
            for fn, src in pure_tasks
        }
        for future in concurrent.futures.as_completed(future_to_source):
            src = future_to_source[future]
            try:
                pairs = future.result()
            except Exception as ex:
                logger.error(f"  {src} 并发异常: {ex}")
                pairs = []

            nc = 0
            for p in pairs:
                e, rec = _make_record(p, src)
                if e and e not in records:
                    records[e] = rec
                    nc += 1
            source_stats[src] = nc
            logger.info(f"  {src} → 新增 {nc} 条")

    logger.info("【快速爬虫】第二阶段：启动 Chrome（idshare001）…")
    driver = make_driver()
    try:
        logger.info("▶ idshare001.me")
        pairs = crawl_idshare001(driver)
        nc = 0
        for p in pairs:
            e, rec = _make_record(p, "idshare001.me")
            if e and e not in records:
                records[e] = rec
                nc += 1
        source_stats["idshare001.me"] = nc
        logger.info(f"  → {nc} 条")

    finally:
        driver.quit()
        logger.info("Chrome 已关闭")

    return records, source_stats


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2 and sys.argv[1] == "test":
        target = sys.argv[2] if len(sys.argv) >= 3 else "all"

        def _print_results(name, results):
            print(f"\n{'='*50}")
            print(f"  站点: {name}  共 {len(results)} 条")
            print(f"{'='*50}")
            for i, r in enumerate(results[:5], 1):
                print(f"  [{i}] email={r.get('email')}  password={r.get('password')}"
                      f"  country={r.get('country')}  checked_at={r.get('checked_at')}")
            if len(results) > 5:
                print(f"  ... 还有 {len(results)-5} 条（只显示前5条）")
            if not results:
                print("  ⚠️  没有爬到任何数据，请检查网络或页面结构")

        if target in ("all", "juzixp"):
            print("\n▶ 测试 ios.juzixp.com ...")
            _print_results("ios.juzixp.com", crawl_ios_juzixp())
        if target in ("all", "us"):
            print("\n▶ 测试 applexp/美区 ...")
            _print_results("applexp/美区", crawl_applexp_us())
        if target in ("all", "jp"):
            print("\n▶ 测试 applexp/日区 ...")
            _print_results("applexp/日区", crawl_applexp_jp())
        if target in ("all", "hk"):
            print("\n▶ 测试 applexp/港区 ...")
            _print_results("applexp/港区", crawl_applexp_hk())
        if target in ("all", "sck"):
            print("\n▶ 测试 applexp/小火箭 ...")
            _print_results("applexp/小火箭", crawl_applexp_shadowrocket())
        if target in ("all", "idshare"):
            print("\n▶ 测试 idshare001.me（启动 Chrome）...")
            _driver = make_driver()
            try:
                _print_results("idshare001.me", crawl_idshare001(_driver))
            finally:
                _driver.quit()
        print("\n✅ 测试完成，未写入任何文件")

    else:
        output_path = os.environ.get("OUTPUT_FILE", "apple_ids.json")
        records, source_stats = crawl_fast()
        result = merge_and_save(records, output_path)
        logger.info(
            "【快速爬虫完成】"
            + " ".join(f"{k}={v}" for k, v in source_stats.items())
            + f" JSON总计={result['total']}"
        )
