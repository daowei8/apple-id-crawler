#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫
- 默认保留账号，只过滤明确标注异常的
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
    r"proton|pm|email|out1ok|hotmail|live|msn)\.[a-z]{2,}\b",
    re.IGNORECASE)

# 只有明确标注这些关键词才认为是异常，其余都保留
STATUS_BAD_KEYWORDS = {"异常", "不可用", "失效", "已失效", "暂无可用", "unavailable", "invalid", "error", "失效账号", "暂无"}

def uid(email: str) -> str:
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]

def is_status_bad(status: str) -> bool:
    """只有明确异常才过滤，空字符串/正常/未知都保留"""
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
        if (e, p) not in seen and len(p) >= 5:
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

# ══════════════════════════════════════════════════════════════
#  各站爬取
# ══════════════════════════════════════════════════════════════

def crawl_free_iosapp_icu(driver) -> list:
    driver.get("https://free.iosapp.icu/")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    # 每个编号卡片
    for card in soup.find_all(["div","section"], recursive=True):
        text = card.get_text(" ", strip=True)
        if len(text) < 20: continue
        me = re.search(r"账[号号][:：\s]*(" + EMAIL_RE.pattern + r")", text, re.I)
        mp = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        ms = re.search(r"状[态態][:：\s]*(\S+)", text)
        mt = re.search(r"检查时间[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        if me and mp:
            status = ms.group(1) if ms else "正常"
            if is_status_bad(status): continue
            results.append({"email": me.group(1).lower(), "password": mp.group(1),
                            "status": status, "checked_at": mt.group(1) if mt else ""})
    return dedup(results)

def crawl_idfree_top(driver) -> list:
    driver.get("https://idfree.top/")
    time.sleep(3)
    # 点击「我已阅读」
    try:
        btn = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH,
            "//button[contains(.,'我已阅读') or contains(.,'继续查看') or contains(.,'查看账号')]")))
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
    except Exception: pass

    # 用JS读所有input的实际值（密码框）
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    pwd_vals = [v for v in
                [driver.execute_script("return arguments[0].value;", i) for i in inputs]
                if v and len(v) >= 5 and "@" not in v]

    emails = EMAIL_RE.findall(driver.page_source)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    page_text = soup.get_text("\n")

    seen, out = set(), []
    for i, email in enumerate(emails):
        e = email.lower()
        if e in seen: continue
        pwd = pwd_vals[i] if i < len(pwd_vals) else (pwd_vals[0] if pwd_vals else "")
        if not pwd: continue
        idx = page_text.find(email)
        ctx = page_text[max(0,idx-50):idx+200] if idx >= 0 else ""
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        ms = re.search(r"(正常|异常|可用)", ctx)
        status = ms.group(1) if ms else "正常"
        if is_status_bad(status): continue
        seen.add(e)
        out.append({"email": e, "password": pwd, "status": status,
                    "checked_at": mt.group(1) if mt else ""})
    return out

def crawl_id_btvda_top(driver) -> list:
    driver.get("https://id.btvda.top/")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","section","article"]):
        text = card.get_text(" ", strip=True)
        if len(text) < 15: continue
        me = EMAIL_RE.search(text)
        mp = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        ms = re.search(r"(正常|异常|可用|不可用)", text)
        if me and mp:
            status = ms.group(1) if ms else "正常"
            if is_status_bad(status): continue
            results.append({"email": me.group().lower(), "password": mp.group(1),
                            "status": status, "checked_at": mt.group(1) if mt else ""})
    if not results:
        results = [p for p in parse_text(soup.get_text("\n")) if not is_status_bad(p.get("status",""))]
    return dedup(results)

def crawl_idshare001(driver) -> list:
    driver.get("https://idshare001.me/goso.html")
    time.sleep(5)
    results = []
    # JS读data属性
    try:
        data = driver.execute_script("""
            var out=[];
            document.querySelectorAll('[data-account],[data-email],[data-id],[data-username]').forEach(function(el){
                var email=el.getAttribute('data-account')||el.getAttribute('data-email')||
                          el.getAttribute('data-id')||el.getAttribute('data-username')||'';
                var pwd=el.getAttribute('data-password')||el.getAttribute('data-pwd')||'';
                if(email&&email.includes('@')) out.push({email:email,pwd:pwd});
            });
            return out;
        """)
        for d in (data or []):
            if d.get("email") and d.get("pwd") and len(d["pwd"]) >= 5:
                results.append({"email": d["email"].lower(), "password": d["pwd"],
                                "status": "正常", "checked_at": ""})
    except Exception: pass

    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for card in soup.find_all(["div","section","li"], recursive=True):
            text = card.get_text(" ", strip=True)
            if len(text) < 15: continue
            me = EMAIL_RE.search(text)
            mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            ms = re.search(r"(正常|异常|检测正常|账号可用|可用)", text)
            if me:
                after = text[me.end():]
                mp = re.search(r"\b([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})\b", after)
                if mp:
                    status = ms.group(1) if ms else "正常"
                    if is_status_bad(status): continue
                    results.append({"email": me.group().lower(), "password": mp.group(1),
                                   "status": status, "checked_at": mt.group(1) if mt else ""})
    return dedup(results)

def crawl_app_iosr_cn(driver) -> list:
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
        if len(text) < 15: continue
        me = EMAIL_RE.search(text)
        if not me: continue
        mp = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        if not mp:
            after = text[me.end():]
            mp2 = re.search(r"\b([A-Za-z0-9]{8,24})\b", after)
            if not mp2: continue
            pwd = mp2.group(1)
        else:
            pwd = mp.group(1)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        ms = re.search(r"(正常|正常使用|可用|Normal)", text, re.I)
        status = ms.group(1) if ms else "正常"
        if is_status_bad(status): continue
        results.append({"email": me.group().lower(), "password": pwd,
                        "status": "正常", "checked_at": mt.group(1) if mt else ""})
    return dedup(results)

def crawl_shadowrocket_best(driver) -> list:
    driver.get("https://shadowrocket.best/")
    time.sleep(4)
    for _ in range(6):
        driver.execute_script("window.scrollBy(0,600);")
        time.sleep(0.7)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","li"], recursive=True):
        if len(list(card.children)) < 2: continue
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
        mt = re.search(r"更[新新]?[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        if not mt: mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        results.append({"email": me.group().lower(), "password": pwd,
                        "status": "正常", "checked_at": mt.group(1) if mt else ""})
    return dedup(results)

def crawl_bocchi2b(driver) -> list:
    driver.get("https://id.bocchi2b.top/")
    time.sleep(3)
    for sel in ["//button[text()='Ok']","//button[text()='OK']","//button[contains(@class,'ok')]",
                "//div[contains(@class,'modal')]//button"]:
        try:
            btn = WebDriverWait(driver, 4).until(EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1); break
        except Exception: pass

    for _ in range(5):
        driver.execute_script("window.scrollBy(0,600);")
        time.sleep(0.7)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","article","li"], recursive=True):
        text = card.get_text(" ", strip=True)
        if len(text) < 15: continue
        me = EMAIL_RE.search(text)
        if not me: continue
        mp = re.search(r"密[码碼][\s:：\*•]+([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
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
        if is_status_bad(status): continue
        results.append({"email": me.group().lower(), "password": pwd,
                        "status": "正常", "checked_at": mt.group(1) if mt else ""})
    return dedup(results)

def crawl_ip_share(driver) -> list:
    driver.get("http://139.196.183.52/share/DZhBvnglEU")
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","li"], recursive=True):
        text = card.get_text(" ", strip=True)
        if len(text) < 15: continue
        me = EMAIL_RE.search(text)
        if not me: continue
        after = text[me.end():]
        mp = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
        if not mp: continue
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        ms = re.search(r"(正常|异常|可用)", text)
        status = ms.group(1) if ms else "正常"
        if is_status_bad(status): continue
        results.append({"email": me.group().lower(), "password": mp.group(1),
                        "status": "正常", "checked_at": mt.group(1) if mt else ""})
    return dedup(results)

def crawl_nodeba(driver) -> list:
    driver.get("https://nodeba.com/")
    time.sleep(4)
    results = []
    try:
        links = driver.find_elements(By.CSS_SELECTOR, "article a, h2 a, h3 a, .post-title a, .entry-title a")
        article_url = None
        for link in links:
            href = link.get_attribute("href") or ""
            txt  = link.text or ""
            if "nodeba.com" in href and href != "https://nodeba.com/" and \
               any(kw in txt for kw in ["Apple","apple","ID","id","账号","共享"]):
                article_url = href; break
        if not article_url and links:
            article_url = links[0].get_attribute("href")
        logger.info(f"  nodeba文章: {article_url}")
        driver.get(article_url)
        time.sleep(4)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        pairs = parse_text(soup.get_text("\n"))
        results = [p for p in pairs if not is_status_bad(p.get("status",""))]
    except Exception as e:
        logger.error(f"  nodeba失败: {e}")
    return dedup(results)

def crawl_tkbaohe(driver) -> list:
    driver.get("https://tkbaohe.com/Shadowrocket/")
    time.sleep(5)
    results = []
    try:
        data = driver.execute_script("""
            var out=[];
            var cards=document.querySelectorAll('.card,.item,article,[class*="account"],[class*="id-card"]');
            if(!cards.length) cards=document.querySelectorAll('div');
            cards.forEach(function(card){
                var txt=card.innerText||'';
                var em=txt.match(/[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i);
                if(!em) return;
                var pwdVal='';
                card.querySelectorAll('input').forEach(function(inp){
                    var v=inp.value;
                    if(v&&v.length>=5&&!v.includes('@')) pwdVal=v;
                });
                var tm=txt.match(/20\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d/);
                var st=txt.match(/正常|可用|Normal/i);
                if(em&&pwdVal) out.push({email:em[0],pwd:pwdVal,
                    time:tm?tm[0]:'',status:st?st[0]:'正常'});
            });
            return out;
        """)
        for d in (data or []):
            if d.get("email") and d.get("pwd") and not is_status_bad(d.get("status","")):
                results.append({"email": d["email"].lower(), "password": d["pwd"],
                               "status": "正常", "checked_at": d.get("time","")})
    except Exception as e:
        logger.error(f"  tkbaohe失败: {e}")
    if not results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        pairs = parse_text(soup.get_text("\n"))
        results = [p for p in pairs if not is_status_bad(p.get("status",""))]
    return dedup(results)

# ══════════════════════════════════════════════════════════════
#  站点列表
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
                if len(set(pwd)) < 2:  # 全相同字符过滤
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

    # 按检查时间降序，没检查时间的排最后
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
