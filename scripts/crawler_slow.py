#!/usr/bin/env python3
"""
Apple ID 慢速爬虫 — crawler_slow.py
负责低频更新站点（每 7 分钟爬一次）：
  1. ccbaohe.com/appleID  — strategy_mailto_onclick（Cloudflare 保护邮箱 + onclick copy）
  2. tkbaohe.com          — strategy_mailto_onclick（同 ccbaohe 结构）
  3. id.btvda.top         — 直接请求 appleapi.omofunz.com/api/data（返回 list）
  4. id.bocchi2b.top      — requests 静态 onclick 解析 / Selenium API 拦截

注意：idfree.top 已移到 crawler_mid.py，本文件不再爬取。
结果合并写入 apple_ids.json（与 crawler_fast.py / crawler_mid.py 共用同一文件）
合并策略：保留现有 fast/mid 站点账号，用本次新数据覆盖 slow 站点账号。
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

TIME_RE = re.compile(r"(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)")

STATUS_BAD = {"异常", "不可用", "失效", "已失效", "locked", "invalid"}

# idfree.top 已移到 mid，这里只保留真正慢速的站点
SLOW_SOURCES = {"ccbaohe.com/appleID", "tkbaohe.com", "id.btvda.top", "id.bocchi2b.top"}

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


def find_time(text: str) -> str:
    m = TIME_RE.search(text or "")
    return m.group(1).strip() if m else ""


def decode_cfemail(encoded: str) -> str:
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""


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
            if(data && (data.id || data.accounts || data.data)) {
                window.__api_responses.push(data);
            }
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
                inner = data.get("data")
                if isinstance(inner, dict):
                    accounts = inner.get("id") or inner.get("accounts") or []
                    if isinstance(accounts, list) and len(accounts) > 0:
                        return accounts
    logger.info(f"  {site_name} API拦截超时")
    return []


# ══════════════════════════════════════════
# 解析工具
# ══════════════════════════════════════════

def parse_vue_accounts(raw_list: list, site_name="", time_is_utc=False) -> list:
    results = []
    if not raw_list:
        return results
    first = raw_list[0]
    logger.info(f"  {site_name} 样本字段: {list(first.keys()) if isinstance(first, dict) else type(first)}")
    if isinstance(first, dict):
        logger.info(f"  {site_name} 第一条: {dict(list(first.items())[:6])}")
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
            "checked_at": now_cst(),
            "country": country,
        })
    return results


def strategy_mailto_onclick(html: str) -> list:
    """ccbaohe / tkbaohe 专用"""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for card in soup.select(".card-body"):
        email = ""
        cf = card.select_one(".__cf_email__")
        if cf:
            href = cf.get("href", "")
            if href.startswith("mailto:"):
                email = href[7:].strip().lower()
            if not is_valid_email(email):
                enc = cf.get("data-cfemail", "")
                if enc:
                    email = decode_cfemail(enc).lower()
        if not is_valid_email(email):
            for btn in card.select("[data-clipboard-text]"):
                v = btn.get("data-clipboard-text", "").strip().lower()
                if is_valid_email(v):
                    email = v
                    break
        if not is_valid_email(email):
            continue

        pw = ""
        for btn in card.select("button"):
            oc = btn.get("onclick", "")
            if not oc:
                continue
            m = (re.search(r"copy\('([^']{4,64})'\)", oc) or
                 re.search(r'copy\("([^"]{4,64})"\)', oc) or
                 re.search(r"copy\(&#39;([^&]{4,64})&#39;\)", oc) or
                 re.search(r"copy\(([A-Za-z0-9!@#$%^&*()\-_=+]{4,64})\)", oc))
            if not m:
                continue
            val = m.group(1).strip()
            if is_valid_email(val.lower()):
                continue
            if "@" not in val and 4 <= len(val) <= 64:
                pw = val
                break
        if not pw:
            for btn in card.select("[data-clipboard-text]"):
                v = btn.get("data-clipboard-text", "").strip()
                if v and "@" not in v and 4 <= len(v) <= 64:
                    pw = v
                    break
        if not pw or "@" in pw or len(pw) < 4:
            continue

        card_text = card.get_text(" ", strip=True)
        if re.search(r"(异常|失效|不可用|锁定)", card_text, re.I):
            continue

        country = ""
        header = card.find_previous("div", class_="card-header")
        if header:
            country = find_country(header.get_text())
        if not country:
            country = find_country(card_text)

        mt = re.search(
            r"检测时间[：:\s]*(20\d{2}-\d{2}-\d{2}\s\d{2}:\d{2}(?::\d{2})?)", card_text)
        checked_at = mt.group(1) if mt else find_time(card_text)

        results.append({
            "email": email.lower().strip(), "password": pw.strip(),
            "status": "正常",
            "checked_at": checked_at or now_cst(),
            "country": country,
        })
    return results


# ══════════════════════════════════════════
# 站点爬虫
# ══════════════════════════════════════════

def crawl_ccbaohe(driver) -> list:
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
        logger.info(f"  ccbaohe [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  ccbaohe error: {ex}")
        return []


def crawl_tkbaohe(driver) -> list:
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
        logger.info(f"  tkbaohe [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  tkbaohe error: {ex}")
        return []


def crawl_id_btvda_top(driver) -> list:
    """直接请求 appleapi.omofunz.com/api/data（返回 list，Selenium 兜底）"""
    try:
        resp = requests.get("https://appleapi.omofunz.com/api/data",
                            headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            raw = resp.json()
            if isinstance(raw, list) and len(raw) > 0:
                results = parse_vue_accounts(raw, "btvda", time_is_utc=True)
                if results:
                    logger.info(f"  id.btvda.top [direct API] → {len(results)} 条")
                    return dedup(results)
    except Exception as ex:
        logger.debug(f"  btvda direct API: {ex}")

    url = "https://id.btvda.top/"
    try:
        driver.get("about:blank")
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
            {"source": INTERCEPT_JS})
        driver.get(url)
        time.sleep(4)
        close_popups(driver)
        raw = extract_from_vue_api(driver, wait_secs=15, site_name="btvda")
        results = parse_vue_accounts(raw, "btvda", time_is_utc=True)
        logger.info(f"  id.btvda.top [selenium] → {len(results)} 条")
        return dedup(results)
    except Exception as ex:
        logger.error(f"  id.btvda.top error: {ex}")
        return []


def crawl_bocchi2b(driver) -> list:
    url = "https://id.bocchi2b.top/"

    def parse_onclick(html):
        soup = BeautifulSoup(html, "lxml")
        results = []
        btns = soup.find_all("button", onclick=True)
        i = 0
        while i < len(btns) - 1:
            a_m = re.search(r"copyToClipboard\('([^']+)'\)", btns[i].get("onclick", ""))
            b_m = re.search(r"copyToClipboard\('([^']+)'\)", btns[i+1].get("onclick", ""))
            if a_m and b_m:
                email = a_m.group(1).lower().strip()
                pw = b_m.group(1).strip()
                if is_valid_email(email) and pw and "@" not in pw:
                    results.append({"email": email, "password": pw,
                                     "status": "正常", "checked_at": now_cst(), "country": "美国"})
                    i += 2
                    continue
            i += 1
        return results

    html = fetch_html(url)
    if html:
        r = parse_onclick(html)
        if r:
            logger.info(f"  bocchi2b [requests] → {len(r)} 条")
            return dedup(r)

    try:
        driver.get("about:blank")
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
            {"source": INTERCEPT_JS})
        driver.get(url)
        time.sleep(4)
        for _ in range(3):
            close_popups(driver)
            time.sleep(0.5)
        raw = extract_from_vue_api(driver, wait_secs=12, site_name="bocchi2b")
        if raw:
            results = parse_vue_accounts(raw, "bocchi2b", time_is_utc=False)
            logger.info(f"  bocchi2b [API] → {len(results)} 条")
            return dedup(results)
        r = parse_onclick(driver.page_source)
        logger.info(f"  bocchi2b [selenium静态] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  bocchi2b error: {ex}")
        return []


# ══════════════════════════════════════════
# 站点配置
# ══════════════════════════════════════════

SITES = [
    {"name": "ccbaohe.com/appleID",  "fn": crawl_ccbaohe},
    {"name": "tkbaohe.com",          "fn": crawl_tkbaohe},
    {"name": "id.btvda.top",         "fn": crawl_id_btvda_top},
    {"name": "id.bocchi2b.top",      "fn": crawl_bocchi2b},
]


# ══════════════════════════════════════════
# 合并写入 apple_ids.json
# ══════════════════════════════════════════

def merge_and_save(slow_records: dict, output_path: str) -> dict:
    existing_accounts = []
    if Path(output_path).exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                old = json.load(f)
            existing_accounts = [
                a for a in old.get("accounts", [])
                if a.get("source") not in SLOW_SOURCES
            ]
        except Exception as ex:
            logger.warning(f"读取现有文件失败: {ex}")

    merged = {a["email"]: a for a in existing_accounts}
    for e, rec in slow_records.items():
        merged[e] = rec

    # 按来源顺序，每个来源内部按 checked_at 降序（最新在前）
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

def crawl_slow():
    records = {}
    source_stats = {}

    logger.info("【慢速爬虫】启动 Chrome…")
    driver = make_driver()
    try:
        for site in SITES:
            logger.info(f"▶ {site['name']}")
            try:
                pairs = site["fn"](driver)
            except Exception as ex:
                logger.error(f"  {site['name']} 异常: {ex}")
                pairs = []

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
                        "source": site["name"],
                        "updated_at": now_cst(),
                    }
                    nc += 1
                else:
                    existing = records[e]
                    if p.get("country") and not existing.get("country"):
                        existing["country"] = p["country"]
                    new_t = p.get("checked_at", "")
                    old_t = existing.get("checked_at", "")
                    if new_t and new_t > old_t:
                        existing["checked_at"] = new_t

            source_stats[site["name"]] = nc
            logger.info(f"  → 新增 {nc} 条（本次共 {len(records)} 条）")
            time.sleep(1)
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

        print("\n▶ 启动 Chrome ...")
        _driver = make_driver()
        try:
            if target in ("all", "ccbaohe"):
                print("\n▶ 测试 ccbaohe.com/appleID ...")
                _print_results("ccbaohe.com/appleID", crawl_ccbaohe(_driver))
            if target in ("all", "tkbaohe"):
                print("\n▶ 测试 tkbaohe.com ...")
                _print_results("tkbaohe.com", crawl_tkbaohe(_driver))
            if target in ("all", "btvda"):
                print("\n▶ 测试 id.btvda.top ...")
                _print_results("id.btvda.top", crawl_id_btvda_top(_driver))
            if target in ("all", "bocchi"):
                print("\n▶ 测试 id.bocchi2b.top ...")
                _print_results("id.bocchi2b.top", crawl_bocchi2b(_driver))
        finally:
            _driver.quit()
            print("\n Chrome 已关闭")

        print("\n✅ 测试完成，未写入任何文件")

    else:
        output_path = os.environ.get("OUTPUT_FILE", "apple_ids.json")
        records, source_stats = crawl_slow()
        result = merge_and_save(records, output_path)
        logger.info(
            f"【慢速爬虫完成】"
            + " ".join(f"{k}={v}" for k, v in source_stats.items())
            + f" JSON总计={result['total']}"
        )
