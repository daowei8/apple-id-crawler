#!/usr/bin/env python3
"""
Apple ID 中速爬虫 — crawler_mid.py
负责站点（每 2 分钟爬一次）：
  1. idfree.top      — Selenium（有"我已阅读"弹窗必须点击）
  2. fx.xdd.net.tr   — Selenium（有数字人机验证码弹窗，自动读取并填写）

结果合并写入 apple_ids.json（与 fast/slow 共用同一文件）
合并策略：保留现有其他站点账号，用本次新数据覆盖 MID_SOURCES 站点账号。
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
    "eml.ru", "cnap.biz",
}

COUNTRY_RE = re.compile(
    r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|"
    r"俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|"
    r"荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古)"
)

STATUS_BAD = {"异常", "不可用", "失效", "已失效", "locked", "invalid"}

MID_SOURCES = {"idfree.top", "fx.xdd.net.tr"}

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


# ══════════════════════════════════════════
# 解析工具
# ══════════════════════════════════════════

def decode_cfemail(encoded: str) -> str:
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""


def strategy_data_clipboard(html: str) -> list:
    """idfree.top 专用"""
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


def strategy_xdd_account_cards(html: str) -> list:
    """fx.xdd.net.tr 专用解析"""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen = set()

    for card in soup.select(".account-card"):
        email = ""
        copy_btn = card.select_one("button.copy-btn[data-clipboard-text]")
        if copy_btn:
            v = copy_btn.get("data-clipboard-text", "").strip().lower()
            if is_valid_email(v):
                email = v

        if not email:
            cf = card.select_one(".__cf_email__")
            if cf:
                enc = cf.get("data-cfemail", "")
                if enc:
                    email = decode_cfemail(enc).lower().strip()

        if not email or not is_valid_email(email) or email in seen:
            continue

        pw = ""
        pass_btn = card.select_one("button.copy-pass-btn[data-clipboard-text]")
        if pass_btn:
            v = pass_btn.get("data-clipboard-text", "").strip()
            if v and "@" not in v and 4 <= len(v) <= 64:
                pw = v

        if not pw:
            continue

        info_header = card.select_one(".info-header")
        if info_header:
            header_text = info_header.get_text(" ", strip=True)
            if re.search(r"(异常|失效|不可用|locked|invalid)", header_text, re.I):
                continue

        country = ""
        if info_header:
            first_span = info_header.find("span")
            if first_span:
                country = find_country(first_span.get_text(strip=True))
        if not country:
            country = find_country(card.get_text(" ", strip=True))

        checked_at = now_cst()
        time_m = re.search(r"(20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?)",
                           card.get_text(" ", strip=True))
        if time_m:
            checked_at = time_m.group(1).strip()

        seen.add(email)
        results.append({
            "email": email, "password": pw,
            "status": "正常", "checked_at": checked_at,
            "country": country,
        })

    return results


# ══════════════════════════════════════════
# 站点爬虫
# ══════════════════════════════════════════

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


def crawl_xdd_net_tr(driver) -> list:
    """fx.xdd.net.tr 专用：暴力破解数字人机验证码弹窗"""
    from selenium.webdriver.common.keys import Keys

    url = "https://fx.xdd.net.tr/share/wRjpcyhumY"

    # 先试 requests
    html = fetch_html(url, timeout=15)
    if html and "account-card" in html and "copy-btn" in html:
        r = strategy_xdd_account_cards(html)
        if r:
            logger.info(f"  fx.xdd.net.tr [requests] → {len(r)} 条")
            return dedup(r)

    try:
        driver.get(url)
    except Exception as ex:
        logger.error(f"  fx.xdd.net.tr 页面打开失败: {ex}")
        return []

    time.sleep(3)

    verified = False
    for attempt in range(4):
        if len(driver.find_elements(By.CSS_SELECTOR, ".account-card")) > 0:
            verified = True
            logger.info("  xdd 已发现账号卡片，完成验证阶段")
            break

        try:
            js_extract = r"""
            var reg = /验证码[^\d]*(\d{3,6})/;
            var txt = document.body.innerText || "";
            var m1 = txt.match(reg);
            if(m1) return m1[1];
            var inps = document.querySelectorAll('input');
            for(var i=0; i<inps.length; i++){
                var ph = inps[i].placeholder || "";
                var m2 = ph.match(reg);
                if(m2) return m2[1];
            }
            var html = document.body.innerHTML || "";
            var m3 = html.match(reg);
            if(m3) return m3[1];
            return null;
            """

            captcha_code = driver.execute_script(js_extract)

            if not captcha_code:
                logger.warning(f"  xdd 未提取到验证码，等待重试... (第{attempt+1}次)")
                time.sleep(2)
                continue

            logger.info(f"  xdd 成功提取验证码: {captcha_code}")

            inputs = driver.find_elements(By.TAG_NAME, "input")
            target_input = None
            for inp in inputs:
                if inp.is_displayed():
                    target_input = inp
                    break

            if not target_input:
                logger.warning("  xdd 找不到可见的输入框")
                time.sleep(2)
                continue

            target_input.clear()
            target_input.send_keys(captcha_code)
            time.sleep(0.5)

            btns = driver.find_elements(By.TAG_NAME, "button")
            target_btn = None
            for btn in btns:
                if btn.is_displayed() and ("验证" in btn.text or "继续" in btn.text):
                    target_btn = btn
                    break

            if target_btn:
                driver.execute_script("arguments[0].click();", target_btn)
                logger.info("  xdd 已点击验证按钮，等待数据加载...")
                time.sleep(4)
            else:
                logger.warning("  xdd 找不到验证按钮，尝试直接回车提交...")
                target_input.send_keys(Keys.RETURN)
                time.sleep(4)

        except Exception as ex:
            logger.warning(f"  xdd 处理验证码异常: {ex}")
            time.sleep(2)

    if not verified and len(driver.find_elements(By.CSS_SELECTOR, ".account-card")) == 0:
        logger.error("  fx.xdd.net.tr 最终验证失败，未见账号卡片")
        return []

    scroll(driver, n=5)
    time.sleep(1)

    results = strategy_xdd_account_cards(driver.page_source)
    logger.info(f"  fx.xdd.net.tr 最终成功提取: {len(results)} 条")
    return dedup(results)


# ══════════════════════════════════════════
# 站点配置
# ══════════════════════════════════════════

SITES = [
    {"name": "idfree.top",    "fn": crawl_idfree_top},
    {"name": "fx.xdd.net.tr", "fn": crawl_xdd_net_tr},
]


# ══════════════════════════════════════════
# 合并写入 apple_ids.json
# ══════════════════════════════════════════

def merge_and_save(mid_records: dict, output_path: str) -> dict:
    existing_accounts = []
    if Path(output_path).exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                old = json.load(f)
            existing_accounts = [
                a for a in old.get("accounts", [])
                if a.get("source") not in MID_SOURCES
            ]
        except Exception as ex:
            logger.warning(f"读取现有文件失败: {ex}")

    merged = {a["email"]: a for a in existing_accounts}
    for e, rec in mid_records.items():
        merged[e] = rec

    # 按来源顺序，每个来源内部按 checked_at 降序排列
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

def crawl_mid():
    records = {}
    source_stats = {}

    logger.info("【中速爬虫】启动 Chrome…")
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
                e  = p.get("email", "").strip().lower()
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

        print("\n▶ 启动 Chrome（两个站点都需要 Selenium）...")
        _driver = make_driver()
        try:
            if target in ("all", "idfree"):
                print("\n▶ 测试 idfree.top ...")
                _print_results("idfree.top", crawl_idfree_top(_driver))
            if target in ("all", "xdd"):
                print("\n▶ 测试 fx.xdd.net.tr ...")
                _print_results("fx.xdd.net.tr", crawl_xdd_net_tr(_driver))
        finally:
            _driver.quit()
            print("\n Chrome 已关闭")

        print("\n✅ 测试完成，未写入任何文件")

    else:
        output_path = os.environ.get("OUTPUT_FILE", "apple_ids.json")
        records, source_stats = crawl_mid()
        result = merge_and_save(records, output_path)
        logger.info(
            "【中速爬虫完成】"
            + " ".join(f"{k}={v}" for k, v in source_stats.items())
            + f" JSON总计={result['total']}"
        )
