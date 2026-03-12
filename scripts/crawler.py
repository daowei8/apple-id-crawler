#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫
- 针对每个站点专属解析策略
- 过滤掉未购买小火箭的账号（仅保留状态正常的）
- 按检查时间降序排列
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

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|proton|pm|email|out1ok)\.[a-z]{2,}\b",
    re.IGNORECASE)

STATUS_OK  = {"正常", "正常使用", "可用", "normal", "ok", "available"}
STATUS_BAD = {"异常", "不可用", "失效", "已失效", "暂无可用账号", "error", "invalid"}

# ══════════════════════════════════════════════════════════════
#  工具函数
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

def is_status_ok(status: str) -> bool:
    return status.lower().strip() in {s.lower() for s in STATUS_OK}

def norm_time(t: str) -> str:
    """标准化时间字符串"""
    if not t:
        return ""
    t = t.strip()
    # 已经是标准格式
    if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", t):
        return t
    return t

def parse_accounts_from_text(text: str) -> list:
    """从纯文本中提取账号密码对"""
    results, seen = [], set()
    INLINE_RE = re.compile(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
        r"[\s\t]*(?:密码|password|pwd)?[\s:：/|｜,，\t ]+[\s\t]*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)
    PWD_CTX_RE = re.compile(
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)

    for m in INLINE_RE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if (e, p) not in seen:
            seen.add((e, p))
            results.append({"email": e, "password": p, "status": "正常", "checked_at": ""})

    lines = text.splitlines()
    for i, line in enumerate(lines):
        emails = EMAIL_RE.findall(line)
        if not emails: continue
        ctx = "\n".join(lines[max(0,i-2):i+5])
        m = PWD_CTX_RE.search(ctx)
        m_time = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        if m:
            for e in emails:
                k = (e.lower(), m.group(1).strip())
                if k not in seen:
                    seen.add(k)
                    results.append({"email": k[0], "password": k[1],
                                    "status": "正常",
                                    "checked_at": m_time.group(1) if m_time else ""})
    return results

def dedup(lst: list) -> list:
    seen, out = set(), []
    for r in lst:
        if r["email"] not in seen:
            seen.add(r["email"]); out.append(r)
    return out

# ══════════════════════════════════════════════════════════════
#  各站爬取函数
# ══════════════════════════════════════════════════════════════

def crawl_free_iosapp_icu(driver) -> list:
    """free.iosapp.icu — 卡片直接显示"""
    driver.get("https://free.iosapp.icu/")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(True, recursive=True):
        text = card.get_text(" ", strip=True)
        m_e = re.search(r"账[号号][:：\s]*(" + EMAIL_RE.pattern + r")", text, re.I)
        m_p = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        m_s = re.search(r"状[态態][:：\s]*(\S+)", text)
        m_t = re.search(r"检查时间[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        if m_e and m_p:
            status = m_s.group(1) if m_s else "正常"
            if not is_status_ok(status): continue  # 跳过异常账号
            results.append({"email": m_e.group(1).lower(), "password": m_p.group(1),
                            "status": status, "checked_at": m_t.group(1) if m_t else ""})
    return dedup(results)

def crawl_idfree_top(driver) -> list:
    """idfree.top — 点击「我已阅读」，用JS读密码input真实值"""
    driver.get("https://idfree.top/")
    time.sleep(3)
    try:
        btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH,
                "//button[contains(.,'我已阅读') or contains(.,'继续查看') or contains(.,'查看账号')]")))
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
    except Exception:
        pass

    results = []
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # 读所有 input 的真实 value（密码框）
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    pwd_values = [v for v in
                  [driver.execute_script("return arguments[0].value;", i) for i in inputs]
                  if v and len(v) >= 6 and "@" not in v]

    emails = EMAIL_RE.findall(driver.page_source)
    page_text = soup.get_text("\n")

    seen, out = set(), []
    for i, email in enumerate(emails):
        e = email.lower()
        if e in seen: continue
        pwd = pwd_values[i] if i < len(pwd_values) else (pwd_values[0] if pwd_values else "")
        if not pwd: continue
        # 找检查时间
        ctx_idx = page_text.find(email)
        ctx = page_text[max(0, ctx_idx-50):ctx_idx+200] if ctx_idx >= 0 else ""
        m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        m_s = re.search(r"(正常|异常|可用)", ctx)
        status = m_s.group(1) if m_s else "正常"
        if not is_status_ok(status): continue
        seen.add(e)
        out.append({"email": e, "password": pwd, "status": status,
                    "checked_at": m_t.group(1) if m_t else ""})
    return out

def crawl_id_btvda_top(driver) -> list:
    """id.btvda.top"""
    driver.get("https://id.btvda.top/")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","section","article"]):
        text = card.get_text(" ", strip=True)
        m_e = EMAIL_RE.search(text)
        m_p = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_s = re.search(r"(正常|异常|可用|不可用)", text)
        if m_e and m_p:
            status = m_s.group(1) if m_s else "正常"
            if not is_status_ok(status): continue
            results.append({"email": m_e.group().lower(), "password": m_p.group(1),
                            "status": status, "checked_at": m_t.group(1) if m_t else ""})
    if not results:
        pairs = parse_accounts_from_text(soup.get_text("\n"))
        results = [p for p in pairs if is_status_ok(p["status"])]
    return dedup(results)

def crawl_idshare001(driver) -> list:
    """idshare001.me/goso.html — JS读data属性，或文本解析"""
    driver.get("https://idshare001.me/goso.html")
    time.sleep(5)
    results = []

    # 方式1：JS读 data 属性
    try:
        data = driver.execute_script("""
            var out=[];
            document.querySelectorAll('[data-account],[data-email],[data-id],[data-username]').forEach(function(el){
                var email=el.getAttribute('data-account')||el.getAttribute('data-email')||
                          el.getAttribute('data-id')||el.getAttribute('data-username')||'';
                var pwd=el.getAttribute('data-password')||el.getAttribute('data-pwd')||'';
                if(email && email.includes('@')) out.push({email:email,pwd:pwd});
            });
            return out;
        """)
        for d in (data or []):
            if d.get("email") and d.get("pwd"):
                results.append({"email": d["email"].lower(), "password": d["pwd"],
                                "status": "正常", "checked_at": ""})
    except Exception: pass

    # 方式2：找卡片，读账号/密码/状态/时间
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for card in soup.find_all(["div","section","li"], recursive=True):
            text = card.get_text(" ", strip=True)
            m_e = EMAIL_RE.search(text)
            m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            m_s = re.search(r"(正常|异常|检测正常|账号可用)", text)
            if m_e:
                after = text[m_e.end():]
                m_p = re.search(r"\b([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})\b", after)
                if m_p:
                    status = m_s.group(1) if m_s else "正常"
                    if not is_status_ok(status): continue
                    results.append({"email": m_e.group().lower(), "password": m_p.group(1),
                                   "status": status, "checked_at": m_t.group(1) if m_t else ""})
    return dedup(results)

def crawl_app_iosr_cn(driver) -> list:
    """app.iosr.cn — 账号密码完整展示"""
    driver.get("https://app.iosr.cn/tools/apple-shared-id")
    time.sleep(5)
    try:
        driver.find_element(By.XPATH,"//button[contains(.,'刷新')]").click()
        time.sleep(2)
    except Exception: pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","li","article"], recursive=True):
        text = card.get_text(" ", strip=True)
        m_e = EMAIL_RE.search(text)
        if not m_e: continue
        m_p = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        if not m_p:
            after = text[m_e.end():]
            m_p2 = re.search(r"\b([A-Za-z0-9]{8,24})\b", after)
            if not m_p2: continue
            pwd = m_p2.group(1)
        else:
            pwd = m_p.group(1)
        m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_s = re.search(r"(正常|正常使用|可用|Normal)", text, re.I)
        status = m_s.group(1) if m_s else "正常"
        if not is_status_ok(status): continue
        results.append({"email": m_e.group().lower(), "password": pwd,
                        "status": "正常", "checked_at": m_t.group(1) if m_t else ""})
    return dedup(results)

def crawl_shadowrocket_best(driver) -> list:
    """shadowrocket.best — 账号密码完整，含检查时间"""
    driver.get("https://shadowrocket.best/")
    time.sleep(4)
    for _ in range(5):
        driver.execute_script("window.scrollBy(0,600);")
        time.sleep(0.8)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []

    # 找所有账号卡片（账号+密码+时间都在一个块里）
    for card in soup.find_all(["div","li"], recursive=True):
        children = list(card.children)
        if len(children) < 2: continue
        text = card.get_text(" ", strip=True)
        m_e = EMAIL_RE.search(text)
        if not m_e: continue
        # 密码
        m_p = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        if not m_p:
            after = text[m_e.end():]
            m_p2 = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
            if not m_p2: continue
            pwd = m_p2.group(1)
        else:
            pwd = m_p.group(1)
        m_t = re.search(r"更[新新][:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        if not m_t:
            m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_s = re.search(r"(正常|Normal|可用)", text, re.I)
        status = "正常" if m_s else "正常"
        results.append({"email": m_e.group().lower(), "password": pwd,
                        "status": status, "checked_at": m_t.group(1) if m_t else ""})

    return dedup(results)

def crawl_bocchi2b(driver) -> list:
    """id.bocchi2b.top — 关弹窗，账号密码完整显示，含状态和时间"""
    driver.get("https://id.bocchi2b.top/")
    time.sleep(3)
    # 关闭弹窗
    for sel in ["//button[text()='Ok']","//button[text()='OK']","//button[text()='确认']",
                "//button[contains(@class,'ok')]","//div[contains(@class,'modal')]//button"]:
        try:
            btn = WebDriverWait(driver, 4).until(EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1); break
        except Exception: pass

    # 滚动加载
    for _ in range(4):
        driver.execute_script("window.scrollBy(0,600);")
        time.sleep(0.8)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []

    for card in soup.find_all(["div","article","li"], recursive=True):
        text = card.get_text(" ", strip=True)
        m_e = EMAIL_RE.search(text)
        if not m_e: continue
        m_p = re.search(r"密[码碼][\s:：\*•]+([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})", text)
        if not m_p:
            after = text[m_e.end():]
            m_p2 = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
            if not m_p2: continue
            pwd = m_p2.group(1)
        else:
            pwd = m_p.group(1)
        m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_s = re.search(r"(正常|可用|Normal|异常|不可用)", text, re.I)
        status = m_s.group(1) if m_s else "正常"
        if not is_status_ok(status): continue
        results.append({"email": m_e.group().lower(), "password": pwd,
                        "status": "正常", "checked_at": m_t.group(1) if m_t else ""})

    return dedup(results)

def crawl_ip_share(driver) -> list:
    """139.196.183.52 — 账号密码直接显示，含状态"""
    driver.get("http://139.196.183.52/share/DZhBvnglEU")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","li"], recursive=True):
        text = card.get_text(" ", strip=True)
        m_e = EMAIL_RE.search(text)
        if not m_e: continue
        after = text[m_e.end():]
        m_p = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
        if not m_p: continue
        m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_s = re.search(r"(正常|异常|可用)", text)
        status = m_s.group(1) if m_s else "正常"
        if not is_status_ok(status): continue
        results.append({"email": m_e.group().lower(), "password": m_p.group(1),
                        "status": "正常", "checked_at": m_t.group(1) if m_t else ""})
    return dedup(results)

def crawl_nodeba(driver) -> list:
    """nodeba.com — 找最新文章，抓账号密码"""
    driver.get("https://nodeba.com/")
    time.sleep(4)
    results = []
    try:
        # 找包含Apple ID的最新文章
        links = driver.find_elements(By.CSS_SELECTOR, "article a, h2 a, h3 a, .post-title a")
        article_url = None
        for link in links:
            href = link.get_attribute("href") or ""
            txt  = link.text or ""
            if "nodeba.com" in href and href != "https://nodeba.com/" and \
               any(kw in txt for kw in ["Apple","apple","ID","id共享","账号"]):
                article_url = href; break
        if not article_url and links:
            article_url = links[0].get_attribute("href")

        logger.info(f"  nodeba文章: {article_url}")
        driver.get(article_url)
        time.sleep(4)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        text = soup.get_text("\n")
        pairs = parse_accounts_from_text(text)
        results = [p for p in pairs if is_status_ok(p.get("status","正常"))]
    except Exception as e:
        logger.error(f"  nodeba失败: {e}")
    return dedup(results)

def crawl_tkbaohe(driver) -> list:
    """tkbaohe.com/Shadowrocket/ — 账号打码，JS读input value"""
    driver.get("https://tkbaohe.com/Shadowrocket/")
    time.sleep(5)
    results = []

    # JS读所有卡片的账号和密码input
    try:
        data = driver.execute_script("""
            var out=[];
            var cards=document.querySelectorAll('.card,.item,article,[class*="account"],[class*="id-card"],[class*="share"]');
            if(!cards.length) cards=document.querySelectorAll('div');
            cards.forEach(function(card){
                var txt=card.innerText||'';
                var emailM=txt.match(/[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i);
                if(!emailM) return;
                var pwdVal='';
                card.querySelectorAll('input').forEach(function(inp){
                    var v=inp.value;
                    if(v&&v.length>=6&&!v.includes('@')) pwdVal=v;
                });
                var timeM=txt.match(/20\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d/);
                var statM=txt.match(/正常|可用|Normal/i);
                if(emailM && pwdVal) out.push({
                    email:emailM[0],pwd:pwdVal,
                    time:timeM?timeM[0]:'',
                    status:statM?statM[0]:'正常'
                });
            });
            return out;
        """)
        for d in (data or []):
            if d.get("email") and d.get("pwd") and is_status_ok(d.get("status","正常")):
                results.append({"email": d["email"].lower(), "password": d["pwd"],
                               "status": "正常", "checked_at": d.get("time","")})
    except Exception as e:
        logger.error(f"  tkbaohe JS失败: {e}")

    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        pairs = parse_accounts_from_text(soup.get_text("\n"))
        results = [p for p in pairs if is_status_ok(p.get("status","正常"))]

    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  站点配置表
# ══════════════════════════════════════════════════════════════
SITES = [
    {"name": "free.iosapp.icu",   "fn": crawl_free_iosapp_icu},
    {"name": "idfree.top",        "fn": crawl_idfree_top},
    {"name": "id.btvda.top",      "fn": crawl_id_btvda_top},
    {"name": "idshare001.me",     "fn": crawl_idshare001},
    {"name": "app.iosr.cn",       "fn": crawl_app_iosr_cn},
    {"name": "shadowrocket.best", "fn": crawl_shadowrocket_best},
    {"name": "id.bocchi2b.top",   "fn": crawl_bocchi2b},
    {"name": "139.196.183.52",    "fn": crawl_ip_share},
    {"name": "nodeba.com",        "fn": crawl_nodeba},
    {"name": "tkbaohe.com",       "fn": crawl_tkbaohe},
]

# ══════════════════════════════════════════════════════════════
#  主逻辑
# ══════════════════════════════════════════════════════════════
def sort_key(account: dict) -> str:
    """按检查时间降序排序键"""
    t = account.get("checked_at", "")
    if t and re.match(r"\d{4}-\d{2}-\d{2}", t):
        return t
    return account.get("updated_at", "")

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
                # 过滤明显无效密码（全相同字符、纯数字太短等）
                if len(set(pwd)) < 2 or (pwd.isdigit() and len(pwd) < 6):
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

    # 按检查时间降序，没有检查时间的排最后
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
