#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 - 针对每个站点专属解析策略
"""

import re, json, time, hashlib, logging, os
from datetime import datetime, timezone
from typing import Optional

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

# ══════════════════════════════════════════════════════════════
#  工具
# ══════════════════════════════════════════════════════════════
def uid(email: str) -> str:
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]

def make_driver() -> webdriver.Chrome:
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
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    return driver

def wait_and_find(driver, selector, by=By.CSS_SELECTOR, timeout=10):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, selector)))

def simple_get(url, timeout=15) -> Optional[requests.Response]:
    for i in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning(f"  [{i+1}/3] {url}: {e}")
            time.sleep(2**i)
    return None

def parse_text_for_accounts(text: str) -> list:
    """从纯文本中提取账号密码对"""
    results, seen = [], set()
    EMAIL_RE = re.compile(
        r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|proton|pm|email|out1ok)\.[a-z]{2,}\b",
        re.IGNORECASE)
    INLINE_RE = re.compile(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
        r"[\s\t]*(?:密码|password|pwd)?[\s:：/|｜,，\t]+[\s\t]*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)
    PWD_CTX_RE = re.compile(
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)

    for m in INLINE_RE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if (e, p) not in seen:
            seen.add((e, p)); results.append({"email": e, "password": p})

    lines = text.splitlines()
    for i, line in enumerate(lines):
        emails = EMAIL_RE.findall(line)
        if not emails: continue
        ctx = "\n".join(lines[max(0,i-2):i+5])
        m = PWD_CTX_RE.search(ctx)
        if m:
            for e in emails:
                k = (e.lower(), m.group(1).strip())
                if k not in seen:
                    seen.add(k); results.append({"email": k[0], "password": k[1]})
    return results

# ══════════════════════════════════════════════════════════════
#  各站专属爬取函数
# ══════════════════════════════════════════════════════════════

def crawl_free_iosapp_icu(driver) -> list:
    """free.iosapp.icu — 账号密码直接在页面文本里"""
    driver.get("https://free.iosapp.icu/")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    # 每个编号卡片
    for card in soup.find_all(["div","section"], recursive=True):
        text = card.get_text(" ", strip=True)
        m_email = re.search(r"账[号号][:：\s]*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})", text)
        m_pwd   = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        m_status= re.search(r"状[态態][:：\s]*(\S+)", text)
        m_time  = re.search(r"检查时间[:：\s]*([\d\-: ]+)", text)
        if m_email and m_pwd:
            results.append({
                "email":      m_email.group(1).lower(),
                "password":   m_pwd.group(1),
                "status":     m_status.group(1) if m_status else "正常",
                "checked_at": m_time.group(1).strip() if m_time else "",
            })
    # 去重
    seen, out = set(), []
    for r in results:
        if r["email"] not in seen:
            seen.add(r["email"]); out.append(r)
    return out

def crawl_idfree_top(driver) -> list:
    """idfree.top — 需点击「我已阅读」按钮，密码是隐藏的input，用JS读value"""
    driver.get("https://idfree.top/")
    time.sleep(3)
    try:
        btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'我已阅读') or contains(text(),'继续查看')]"))
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
    except Exception:
        pass

    results = []
    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.find_all(["div","section"], class_=re.compile(r"card|account|id", re.I))
    if not cards:
        cards = soup.find_all("div", recursive=False)

    # 用JS直接读所有input的value（密码框）
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    input_values = [driver.execute_script("return arguments[0].value;", inp) for inp in inputs]

    # 找email + 对应密码
    page_text = driver.page_source
    email_re = re.compile(r"[A-Za-z0-9._%+\-]+@(?:icloud|me|gmail|qq|163|hotmail|outlook)\.[a-z]{2,}", re.I)
    emails = email_re.findall(page_text)
    passwords = [v for v in input_values if v and len(v) >= 6 and not "@" in v]

    seen, out = set(), []
    for i, email in enumerate(emails):
        e = email.lower()
        pwd = passwords[i] if i < len(passwords) else (passwords[0] if passwords else "")
        if e not in seen and pwd:
            seen.add(e)
            out.append({"email": e, "password": pwd, "status": "正常", "checked_at": ""})
    return out

def crawl_id_btvda_top(driver) -> list:
    """id.btvda.top — 直接解析"""
    driver.get("https://id.btvda.top/")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    text = soup.get_text("\n")
    pairs = parse_text_for_accounts(text)
    return [{"email": p["email"], "password": p["password"], "status": "正常", "checked_at": ""} for p in pairs]

def crawl_idshare001(driver) -> list:
    """idshare001.me — 点击复制账号/密码按钮，用JS读出data属性"""
    driver.get("https://idshare001.me/goso.html")
    time.sleep(4)
    results = []

    # 尝试从data属性/onclick/href读账号密码
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # 找所有包含账号的卡片
    EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", re.I)

    # 尝试JS读取页面上的数据对象
    try:
        data = driver.execute_script("""
            var results = [];
            document.querySelectorAll('[data-account],[data-email],[data-id]').forEach(function(el){
                results.push({email: el.getAttribute('data-account')||el.getAttribute('data-email')||el.getAttribute('data-id'),
                               pwd: el.getAttribute('data-password')||el.getAttribute('data-pwd')||''});
            });
            return results;
        """)
        for d in (data or []):
            if d.get("email") and "@" in d["email"]:
                results.append({"email": d["email"].lower(), "password": d.get("pwd",""), "status": "正常", "checked_at": ""})
    except Exception:
        pass

    if not results:
        # 直接从文本解析
        pairs = parse_text_for_accounts(soup.get_text("\n"))
        results = [{"email": p["email"], "password": p["password"], "status": "正常", "checked_at": ""} for p in pairs]

    return results

def crawl_app_iosr_cn(driver) -> list:
    """app.iosr.cn — 直接显示，用BeautifulSoup解析卡片"""
    driver.get("https://app.iosr.cn/tools/apple-shared-id")
    time.sleep(5)
    # 点刷新按钮（如果有）
    try:
        btn = driver.find_element(By.XPATH, "//button[contains(text(),'刷新')]")
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(2)
    except Exception:
        pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", re.I)

    # 找所有卡片
    for card in soup.find_all(["div","article"], recursive=True):
        children_text = card.get_text(" ", strip=True)
        m_email = EMAIL_RE.search(children_text)
        if not m_email:
            continue
        # 找密码：卡片内同行/相邻文本
        m_pwd = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", children_text)
        if not m_pwd:
            m_pwd = re.search(r"(?<!\w)([A-Za-z0-9]{8,20})(?!\w)", children_text.replace(m_email.group(),""))
        m_time = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", children_text)
        m_status = re.search(r"(正常|正常使用|可用|异常|不可用|Normal)", children_text)
        if m_email and m_pwd:
            results.append({
                "email":      m_email.group().lower(),
                "password":   m_pwd.group(1),
                "status":     m_status.group(1) if m_status else "正常",
                "checked_at": m_time.group(1) if m_time else "",
            })

    seen, out = set(), []
    for r in results:
        if r["email"] not in seen:
            seen.add(r["email"]); out.append(r)
    return out

def crawl_shadowrocket_best(driver) -> list:
    """shadowrocket.best — 账号密码完整显示在卡片里"""
    driver.get("https://shadowrocket.best/")
    time.sleep(4)
    # 滚动加载更多
    for _ in range(3):
        driver.execute_script("window.scrollBy(0, 600);")
        time.sleep(1)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", re.I)

    for card in soup.find_all(["div","li"], recursive=True):
        text = card.get_text(" ", strip=True)
        m_email = EMAIL_RE.search(text)
        if not m_email: continue
        # 密码在邮箱下面一行
        m_pwd = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        if not m_pwd:
            # 找邮箱后的第一个类密码字符串
            after_email = text[m_email.end():]
            m_pwd2 = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after_email)
            if m_pwd2 and len(m_pwd2.group(1)) >= 6:
                pwd = m_pwd2.group(1)
            else:
                continue
        else:
            pwd = m_pwd.group(1)

        m_time = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_status = re.search(r"(正常|正常使用|可用|Normal)", text)
        results.append({
            "email":      m_email.group().lower(),
            "password":   pwd,
            "status":     m_status.group(1) if m_status else "正常",
            "checked_at": m_time.group(1) if m_time else "",
        })

    seen, out = set(), []
    for r in results:
        if r["email"] not in seen:
            seen.add(r["email"]); out.append(r)
    return out

def crawl_bocchi2b(driver) -> list:
    """id.bocchi2b.top — 弹窗关掉，账号密码直接显示"""
    driver.get("https://id.bocchi2b.top/")
    time.sleep(3)
    # 关闭弹窗
    try:
        ok_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Ok' or text()='OK' or text()='确认' or text()='关闭']"))
        )
        driver.execute_script("arguments[0].click();", ok_btn)
        time.sleep(1)
    except Exception:
        pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", re.I)

    for card in soup.find_all(["div","article"], recursive=True):
        text = card.get_text(" ", strip=True)
        m_email = EMAIL_RE.search(text)
        if not m_email: continue
        m_pwd = re.search(r"密[码碼][\s:：\*]+([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        if not m_pwd:
            after = text[m_email.end():]
            m2 = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
            if not m2: continue
            pwd = m2.group(1)
        else:
            pwd = m_pwd.group(1)
        m_time   = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_status = re.search(r"(正常|可用|Normal)", text)
        results.append({
            "email":      m_email.group().lower(),
            "password":   pwd,
            "status":     m_status.group(1) if m_status else "正常",
            "checked_at": m_time.group(1) if m_time else "",
        })

    seen, out = set(), []
    for r in results:
        if r["email"] not in seen:
            seen.add(r["email"]); out.append(r)
    return out

def crawl_ip_share(driver) -> list:
    """139.196.183.52/share/DZhBvnglEU — 完整账号直接显示"""
    driver.get("http://139.196.183.52/share/DZhBvnglEU")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", re.I)

    for card in soup.find_all(["div"], recursive=True):
        text = card.get_text(" ", strip=True)
        m_email = EMAIL_RE.search(text)
        if not m_email: continue
        after = text[m_email.end():]
        m_pwd = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
        if not m_pwd: continue
        m_time   = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_status = re.search(r"(正常|异常|可用)", text)
        results.append({
            "email":      m_email.group().lower(),
            "password":   m_pwd.group(1),
            "status":     m_status.group(1) if m_status else "正常",
            "checked_at": m_time.group(1) if m_time else "",
        })

    seen, out = set(), []
    for r in results:
        if r["email"] not in seen:
            seen.add(r["email"]); out.append(r)
    return out

def crawl_nodeba(driver) -> list:
    """nodeba.com — 博客，找最新文章点进去，账号格式：账号: xxx@xxx 密码: yyy"""
    driver.get("https://nodeba.com/")
    time.sleep(4)
    results = []
    try:
        # 找最新一篇包含 Apple ID 的文章链接
        links = driver.find_elements(By.CSS_SELECTOR, "a")
        article_url = None
        for link in links:
            href = link.get_attribute("href") or ""
            text = link.text
            if "Apple" in text or "apple" in text or "ID" in text or "id" in text.lower():
                if "nodeba.com" in href and href != "https://nodeba.com/":
                    article_url = href
                    break
        if not article_url:
            # 直接点第一篇文章
            first = driver.find_element(By.CSS_SELECTOR, "article a, .post a, h2 a, h3 a")
            article_url = first.get_attribute("href")

        logger.info(f"  nodeba 文章: {article_url}")
        driver.get(article_url)
        time.sleep(4)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        text = soup.get_text("\n")
        pairs = parse_text_for_accounts(text)
        results = [{"email": p["email"], "password": p["password"], "status": "正常", "checked_at": ""} for p in pairs]
    except Exception as e:
        logger.error(f"  nodeba 失败: {e}")
    return results

def crawl_tkbaohe(driver) -> list:
    """tkbaohe.com/Shadowrocket/ — 账号打码，但密码区域可以用JS读input value"""
    driver.get("https://tkbaohe.com/Shadowrocket/")
    time.sleep(5)
    results = []
    try:
        # 用JS读所有账号input和密码input的value
        data = driver.execute_script("""
            var out = [];
            var cards = document.querySelectorAll('.card, .item, [class*=account], [class*=id]');
            cards.forEach(function(card){
                var inputs = card.querySelectorAll('input');
                var texts  = card.innerText || '';
                var emailMatch = texts.match(/[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i);
                var pwdVal = '';
                inputs.forEach(function(inp){
                    var v = inp.value;
                    if(v && v.length >= 6 && !v.includes('@')) pwdVal = v;
                });
                if(emailMatch && pwdVal) out.push({email: emailMatch[0], password: pwdVal});
            });
            return out;
        """)
        for d in (data or []):
            if d.get("email") and d.get("password"):
                results.append({"email": d["email"].lower(), "password": d["password"], "status": "正常", "checked_at": ""})
    except Exception as e:
        logger.error(f"  tkbaohe JS读取失败: {e}")

    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        pairs = parse_text_for_accounts(soup.get_text("\n"))
        results = [{"email": p["email"], "password": p["password"], "status": "正常", "checked_at": ""} for p in pairs]

    seen, out = set(), []
    for r in results:
        if r["email"] not in seen:
            seen.add(r["email"]); out.append(r)
    return out

# ══════════════════════════════════════════════════════════════
#  站点配置表
# ══════════════════════════════════════════════════════════════
SITES = [
    {"name": "free.iosapp.icu",  "fn": crawl_free_iosapp_icu},
    {"name": "idfree.top",       "fn": crawl_idfree_top},
    {"name": "id.btvda.top",     "fn": crawl_id_btvda_top},
    {"name": "idshare001.me",    "fn": crawl_idshare001},
    {"name": "app.iosr.cn",      "fn": crawl_app_iosr_cn},
    {"name": "shadowrocket.best","fn": crawl_shadowrocket_best},
    {"name": "id.bocchi2b.top",  "fn": crawl_bocchi2b},
    {"name": "139.196.183.52",   "fn": crawl_ip_share},
    {"name": "nodeba.com",       "fn": crawl_nodeba},
    {"name": "tkbaohe.com",      "fn": crawl_tkbaohe},
]

# ══════════════════════════════════════════════════════════════
#  主逻辑
# ══════════════════════════════════════════════════════════════
def crawl_all() -> dict:
    seen: dict         = {}
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
                email = p.get("email", "").strip().lower()
                pwd   = p.get("password", "").strip()
                if not email or not pwd or "@" not in email or len(pwd) < 4:
                    continue
                if email not in seen:
                    seen[email] = {
                        "id":         uid(email),
                        "email":      email,
                        "password":   pwd,
                        "status":     p.get("status", "正常"),
                        "checked_at": p.get("checked_at", ""),
                        "source":     site["name"],
                        "updated_at": now_iso,
                    }
                    new_count += 1

            source_stats[site["name"]] = new_count
            logger.info(f"  → 新增 {new_count} 条（去重后共 {len(seen)} 条）")
            time.sleep(2)

    finally:
        driver.quit()
        logger.info("浏览器已关闭")

    accounts = sorted(seen.values(), key=lambda x: x["updated_at"], reverse=True)
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
