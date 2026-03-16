#!/usr/bin/env python3
"""
Apple ID 快速爬虫 — crawler_fast.py
负责高频更新站点（每 1 分钟爬一次）：
  1. idshare001.me  — 直接请求 /node/getid.php?getid=1 和 getid=2（纯 requests）
  2. idfree.top     — Selenium（有"我已阅读"弹窗必须点击）

结果合并写入 apple_ids.json（与 crawler_slow.py 共用同一文件）
合并策略：保留现有 slow 站点账号，用本次新数据覆盖 fast 站点账号。
"""

import re, json, time, hashlib, logging, os
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
}

COUNTRY_RE = re.compile(
    r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|"
    r"俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|"
    r"荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古)"
)

STATUS_BAD = {"异常", "不可用", "失效", "已失效", "locked", "invalid"}

# 本爬虫负责的站点名（合并时用于清除旧数据）
FAST_SOURCES = {"idshare001.me"}

SITE_ORDER = [
    "idfree.top", "idshare001.me",
    "ccbaohe.com/appleID", "tkbaohe.com",
    "id.btvda.top", "id.bocchi2b.top",
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
    if len(local) < 4:
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
    if raw_list:
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

    # 方法1：id 精确配对
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

    # 方法2：.card-body 内 .copy-btn / .copy-pass-btn
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
    """逐卡片点击账号/密码按钮，用剪贴板钩子配对"""
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
    """
    直接 requests /node/getid.php?getid=2 和 getid=1（1 分钟更新一次）
    Selenium 仅兜底。
    """
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
    """
    idfree.top — 必须 Selenium 点击弹窗。
    先尝试 requests 静态解析，大概率因弹窗失败，再 Selenium。
    """
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
        # 通用剪贴板兜底
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


# ══════════════════════════════════════════
# 合并写入 apple_ids.json
# ══════════════════════════════════════════

def merge_and_save(fast_records: dict, output_path: str) -> dict:
    """
    1. 读取现有 apple_ids.json
    2. 保留非 FAST_SOURCES 的账号（slow 站点）
    3. 用 fast_records 覆盖 fast 站点账号
    4. 写回文件
    """
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
                       a.get("checked_at", "") or "")
    )

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

def crawl_fast():
    records = {}
    source_stats = {}

    logger.info("【快速爬虫】启动 Chrome…")
    driver = make_driver()
    try:
        # ── idshare001（优先纯 requests）──────────────────
        logger.info("▶ idshare001.me")
        pairs = crawl_idshare001(driver)
        nc = 0
        for p in pairs:
            e = p.get("email", "").strip().lower()
            pw = p.get("password", "").strip()
            if not is_valid_email(e) or not pw or len(pw) < 4 or len(pw) > 64:
                continue
            if len(set(pw)) < 2:
                continue
            if "&amp;" in pw:
                pw = pw.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
            if e not in records:
                records[e] = {
                    "id": uid(e), "email": e, "password": pw,
                    "status": p.get("status", "正常"),
                    "country": p.get("country", ""),
                    "checked_at": p.get("checked_at", now_cst()),
                    "source": "idshare001.me",
                    "updated_at": now_cst(),
                }
                nc += 1
        source_stats["idshare001.me"] = nc
        logger.info(f"  → {nc} 条")


    finally:
        driver.quit()
        logger.info("Chrome 已关闭")

    return records, source_stats


if __name__ == "__main__":
    output_path = os.environ.get("OUTPUT_FILE", "apple_ids.json")
    records, source_stats = crawl_fast()
    result = merge_and_save(records, output_path)
    logger.info(
        f"【快速爬虫完成】idshare001={source_stats.get('idshare001.me', 0)} "
        f"JSON总计={result['total']}"
    )
