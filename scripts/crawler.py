#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 (加强版)
- 基于 95ge.py，修复所有站点抓取不完整问题
- 时间使用中国时间 UTC+8
- 所有域名保持完整路径
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

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|"
    r"proton|pm|email|out1ok|live|msn)\.[a-z]{2,}\b",
    re.IGNORECASE)

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
        {"source":"Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
    return driver

def dedup(lst):
    seen, out = set(), []
    for r in lst:
        e = r.get("email","").lower().strip()
        if e and e not in seen:
            seen.add(e); out.append(r)
    return out

def parse_text(text):
    results, seen = [], set()
    INLINE = re.compile(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
        r"[\s\t]*(?:密码|password|pwd)?[\s:：/|｜,，\t ]*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})", re.IGNORECASE)
    CTX_PWD = re.compile(
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)
    for m in INLINE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if (e,p) not in seen and len(p)>=5:
            seen.add((e,p))
            results.append({"email":e,"password":p,"status":"正常","checked_at":""})
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
                if k not in seen and len(k[1])>=5:
                    seen.add(k)
                    results.append({"email":k[0],"password":k[1],
                                    "status":"正常","checked_at":mt.group(1) if mt else ""})
    return results

JS_INPUTS = """
var out=[];
document.querySelectorAll('input').forEach(function(inp){
    var v=inp.value||'';
    if(v&&v.length>=5&&!v.includes('@')){
        var p=inp.closest('[class]')||inp.parentElement;
        var txt=p?p.innerText:'';
        var em=txt.match(/[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i);
        if(em) out.push({email:em[0],pwd:v,txt:txt});
    }
});
return out;
"""

def from_inputs(driver):
    try:
        data = driver.execute_script(JS_INPUTS)
        results, seen = [], set()
        for d in (data or []):
            e = d.get("email","").lower()
            p = d.get("pwd","")
            if e and p and "@" in e and e not in seen and len(p)>=5:
                seen.add(e)
                txt = d.get("txt","")
                mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", txt)
                results.append({"email":e,"password":p,"status":"正常",
                                "checked_at":mt.group(1) if mt else ""})
        return results
    except Exception:
        return []

def scroll(driver, n=8):
    for _ in range(n):
        driver.execute_script("window.scrollBy(0,700);")
        time.sleep(0.6)

def generic_parse(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    for card in soup.find_all(["div","li","article","section","tr"], recursive=True):
        text = card.get_text(" ", strip=True)
        if len(text)<15: continue
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
        results.append({"email":me.group().lower(),"password":pwd,
                        "status":"正常","checked_at":mt.group(1) if mt else ""})
    return dedup(results)


# ── 通用：注入hook拦截复制事件，然后批量点复制按钮 ──────────────
HOOK_JS = """
window.__copied = window.__copied || [];
var _orig = navigator.clipboard.writeText.bind(navigator.clipboard);
navigator.clipboard.writeText = function(text){
    window.__copied.push(text);
    return _orig(text);
};
document.addEventListener('copy', function(e){
    try{
        var t = e.clipboardData && e.clipboardData.getData('text');
        if(t) window.__copied.push(t);
    }catch(ex){}
});
"""

def click_all_copy_btns(driver, max_clicks=200):
    """注入hook后批量点所有复制按钮，收集账号+密码"""
    driver.execute_script(HOOK_JS)
    time.sleep(0.5)
    
    # 点所有复制账号/复制密码按钮
    btns = driver.find_elements(By.XPATH,
        "//button[contains(.,'复制账号') or contains(.,'复制帐号') or contains(.,'复制号') or contains(.,'账号')]"
        " | //button[contains(.,'复制密码') or contains(.,'密码')]"
        " | //a[contains(.,'复制账号')] | //a[contains(.,'复制密码')]"
    )
    
    emails, pwds = [], []
    for btn in btns[:max_clicks]:
        try:
            before = len(driver.execute_script("return window.__copied||[]"))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.15)
            after = driver.execute_script("return window.__copied||[]")
            if len(after) > before:
                val = after[-1].strip()
                if "@" in val:
                    emails.append(val.lower())
                elif len(val) >= 5:
                    pwds.append(val)
        except Exception:
            pass
    
    # 配对：emails和pwds交替出现（先账号后密码）
    results = []
    seen = set()
    # 方式1：两两配对
    i = 0
    while i < len(emails):
        e = emails[i]
        # 找对应密码：pwds里和emails等长或按顺序
        p = pwds[i] if i < len(pwds) else ""
        if e and p and e not in seen and len(p) >= 5:
            seen.add(e)
            results.append({"email": e, "password": p, "status": "正常", "checked_at": "", "country": "美国"})
        i += 1
    
    # 方式2：如果配对失败，从__copied里按顺序重新配对
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
                                     "checked_at": "", "country": "美国"})
                i += 2
            else:
                i += 1
    
    return results


def crawl_ccbaohe(driver):
    driver.get("https://ccbaohe.com/appleID")
    time.sleep(8)
    scroll(driver)
    time.sleep(2)
    
    results = click_all_copy_btns(driver)
    
    # 同时用文本解析补充国家信息
    if results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        country_map = {}
        for card in soup.find_all(["div","li","tr"], class_=True):
            text = card.get_text(" ", strip=True)
            me = EMAIL_RE.search(text)
            if not me: continue
            mc = re.search(r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|中国大陆)", text)
            if mc: country_map[me.group().lower()] = mc.group(1)
        for r in results:
            r["country"] = country_map.get(r["email"], "美国")
    
    if not results:
        # 降级：直接解析页面文本
        soup = BeautifulSoup(driver.page_source, "html.parser")
        results = generic_parse(driver)
    
    logger.info(f"  ccbaohe 抓到: {len(results)}")
    return dedup(results)


def crawl_tkbaohe(driver):
    driver.get("https://tkbaohe.com/Shadowrocket/")
    time.sleep(8)
    scroll(driver)
    time.sleep(2)
    
    results = click_all_copy_btns(driver)
    
    if results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        country_map = {}
        for card in soup.find_all(["div","li","tr"], class_=True):
            text = card.get_text(" ", strip=True)
            me = EMAIL_RE.search(text)
            if not me: continue
            mc = re.search(r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|中国大陆)", text)
            mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            if me.group().lower() in [r["email"] for r in results]:
                country_map[me.group().lower()] = (mc.group(1) if mc else "美国", mt.group(1) if mt else "")
        for r in results:
            info = country_map.get(r["email"], ("美国", ""))
            r["country"] = info[0]
            r["checked_at"] = info[1]
    
    if not results:
        results = generic_parse(driver)
    
    logger.info(f"  tkbaohe 抓到: {len(results)}")
    return dedup(results)


def crawl_idfree_top(driver):
    driver.get("https://idfree.top/")
    time.sleep(5)
    for sel in ["//button[contains(.,'我已阅读')]","//button[contains(.,'继续查看')]",
                "//button[contains(.,'查看账号')]","//button[contains(.,'知道了')]"]:
        try:
            btn = WebDriverWait(driver,4).until(EC.element_to_be_clickable((By.XPATH,sel)))
            driver.execute_script("arguments[0].click();",btn); time.sleep(3); break
        except Exception: pass
    scroll(driver)
    
    results = click_all_copy_btns(driver)
    if not results:
        results = from_inputs(driver)
    
    logger.info(f"  idfree_top 抓到: {len(results)}")
    return dedup(results)


def crawl_id_btvda_top(driver):
    driver.get("https://id.btvda.top/")
    time.sleep(6)
    scroll(driver)
    time.sleep(2)
    
    results = click_all_copy_btns(driver)
    
    if results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        country_map = {}
        for card in soup.find_all(["div","li"], class_=True):
            text = card.get_text(" ", strip=True)
            me = EMAIL_RE.search(text)
            if not me: continue
            mc = re.search(r"(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚)", text)
            mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            country_map[me.group().lower()] = (mc.group(1) if mc else "美国", mt.group(1) if mt else "")
        for r in results:
            info = country_map.get(r["email"], ("美国",""))
            r["country"] = info[0]; r["checked_at"] = info[1]
    
    if not results:
        results = from_inputs(driver) or generic_parse(driver)
    
    logger.info(f"  id_btvda_top 抓到: {len(results)}")
    return dedup(results)


def crawl_idshare001(driver):
    driver.get("https://idshare001.me/goso.html")
    time.sleep(6)
    
    results = click_all_copy_btns(driver)
    
    if not results:
        # 备用：data属性
        try:
            data = driver.execute_script("""
                var out=[];
                document.querySelectorAll('[data-account],[data-email],[data-password],[data-pwd]').forEach(function(el){
                    var em=el.getAttribute('data-account')||el.getAttribute('data-email')||'';
                    var pw=el.getAttribute('data-password')||el.getAttribute('data-pwd')||'';
                    if(em&&em.includes('@')&&pw&&pw!=='undefined'&&pw.length>=5)
                        out.push({email:em,pwd:pw});
                });
                return out;
            """)
            seen = set()
            for d in (data or []):
                e=(d.get("email") or "").lower(); p=d.get("pwd") or ""
                if e and p and "@" in e and len(p)>=5 and e not in seen:
                    seen.add(e)
                    results.append({"email":e,"password":p,"status":"正常","checked_at":"","country":"美国"})
        except Exception: pass
    
    if not results:
        results = from_inputs(driver) or generic_parse(driver)
    
    logger.info(f"  idshare001 抓到: {len(results)}")
    return dedup(results)


def crawl_bocchi2b(driver):
    driver.get("https://id.bocchi2b.top/")
    time.sleep(4)
    for sel in ["//button[text()='Ok']","//button[text()='OK']","//button[text()='确定']",
                "//div[contains(@class,'modal')]//button"]:
        try:
            btn = WebDriverWait(driver,4).until(EC.element_to_be_clickable((By.XPATH,sel)))
            driver.execute_script("arguments[0].click();",btn); time.sleep(1); break
        except Exception: pass
    scroll(driver)
    time.sleep(2)
    
    results = click_all_copy_btns(driver)
    
    if results:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        country_map = {}
        for card in soup.find_all(["div","li"], class_=True):
            text = card.get_text(" ", strip=True)
            me = EMAIL_RE.search(text)
            if not me: continue
            mc = re.search(r"(香港|日本|美国|英国|台湾|韩国|越南|蒙古|未知)", text)
            mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            country_map[me.group().lower()] = (mc.group(1) if mc else "美国", mt.group(1) if mt else "")
        for r in results:
            info = country_map.get(r["email"], ("美国",""))
            r["country"] = info[0]; r["checked_at"] = info[1]
    
    if not results:
        results = from_inputs(driver) or generic_parse(driver)
    
    logger.info(f"  bocchi2b 抓到: {len(results)}")
    return dedup(results)




 
# ──────────────────────────────────────────
# shadowrocket.best/
# ──────────────────────────────────────────
def crawl_shadowrocket_best(driver):
    driver.get("https://shadowrocket.best/")
    time.sleep(6)
    # 多次滚动确保所有卡片加载
    last_count = 0
    for _ in range(20):
        driver.execute_script("window.scrollBy(0, 600);")
        time.sleep(0.8)
        cards = driver.find_elements(By.CSS_SELECTOR, ".card,.id-card,.account-card,[class*='card'],[class*='item']")
        if len(cards) == last_count:
            break
        last_count = len(cards)
    driver.execute_script("window.scrollTo(0,0)")
    time.sleep(1)

    results = []
    seen = set()
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    # shadowrocket.best 的结构：每个账号是一个独立卡片
    # 账号、密码、更新时间都在卡片里，密码标签可能是"密码:"或直接显示
    for card in soup.find_all(["div", "li"], recursive=True):
        children = list(card.children)
        if len(children) < 2: continue
        text = card.get_text(" ", strip=True)
        if len(text) < 20 or len(text) > 500: continue
        me = EMAIL_RE.search(text)
        if not me: continue
        e = me.group().lower()
        if e in seen: continue
        
        mp = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        if not mp:
            after = text[me.end():]
            # 找账号后面的密码（6-24位，含字母）
            mp2 = re.search(r"\b([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})\b", after)
            if not mp2: continue
            pwd = mp2.group(1)
            # 排除时间戳
            if re.match(r"^\d{4}-\d{2}-\d{2}$", pwd): continue
        else:
            pwd = mp.group(1)
        
        mt = re.search(r"更[新]?[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        if not mt: mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        seen.add(e)
        results.append({"email": e, "password": pwd, "status": "正常",
                        "checked_at": mt.group(1) if mt else "", "country": "美国"})
    
    if not results:
        results = from_inputs(driver)
    return dedup(results)
    
# ──────────────────────────────────────────
# free.iosapp.icu/
# ──────────────────────────────────────────
def crawl_free_iosapp_icu(driver):
    driver.get("https://free.iosapp.icu/")
    time.sleep(5)
    scroll(driver)
    
    results = []
    seen = set()
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    # 每个账号块：编号N，账号: xxx，密码: xxx，状态: 账号可用
    for block in soup.find_all(["div", "section", "article"], recursive=True):
        text = block.get_text(" ", strip=True)
        if len(text) < 20 or len(text) > 800: continue
        me = re.search(r"账[号号][:：\s]*(" + EMAIL_RE.pattern + r")", text, re.I)
        if not me:
            me = EMAIL_RE.search(text)
        if not me: continue
        e = me.group(1 if me.lastindex else 0).lower() if "账" in text[:me.start()+5] else me.group().lower()
        if "@" not in e or e in seen: continue
        
        mp = re.search(r"密[码碼][:：\s]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        if not mp: continue
        pwd = mp.group(1)
        if re.match(r"^20\d\d-\d\d-\d\d$", pwd): continue
        
        ms = re.search(r"状[态態][:：\s]*(\S+)", text)
        status = ms.group(1) if ms else "正常"
        if bad(status): continue
        
        mt = re.search(r"检查时间[:：\s]*(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        seen.add(e)
        results.append({"email": e, "password": pwd, "status": "正常",
                        "checked_at": mt.group(1) if mt else "", "country": "美国"})
    
    if not results:
        results = from_inputs(driver)
    return dedup(results)
    

 
# ──────────────────────────────────────────
# app.iosr.cn/tools/apple-shared-id
# ──────────────────────────────────────────
def crawl_app_iosr_cn(driver):
    driver.get("https://app.iosr.cn/tools/apple-shared-id")
    time.sleep(7)
    # 点刷新按钮
    try:
        driver.find_element(By.XPATH, "//button[contains(.,'刷新')]").click()
        time.sleep(4)
    except Exception: pass
    scroll(driver)
    
    results = []
    seen = set()
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    # 结构：每张卡片有「账号」「密码」label + 对应值
    for card in soup.find_all(["div", "li", "article"], class_=True):
        text = card.get_text(" ", strip=True)
        if len(text) < 20 or len(text) > 600: continue
        me = EMAIL_RE.search(text)
        if not me: continue
        e = me.group().lower()
        if e in seen: continue
        
        # 密码：「密码」标签后的值，允许特殊字符（如V&hmp:hRi06t）
        mp = re.search(r"密[码碼][\s:：]*([^\s]{5,32})", text)
        if not mp:
            # 找账号后面第一个非时间、非状态的词
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
        if not ms: continue  # app.iosr.cn 状态必须明确是正常才要
        
        # 国家
        mc = re.search(r"(美国|英国|日本|香港|台湾|韩国|澳大利亚|越南|新加坡|中国大陆)", text)
        country = mc.group(1) if mc else "美国"
        
        seen.add(e)
        results.append({"email": e, "password": pwd, "status": "正常",
                        "checked_at": mt.group(1) if mt else "",
                        "country": country})
    
    if not results:
        results = from_inputs(driver)
    return dedup(results)
    

# ──────────────────────────────────────────
# 139.196.183.52/share/DZhBvnglEU
# ──────────────────────────────────────────
def crawl_ip_share(driver):
    driver.get("http://139.196.183.52/share/DZhBvnglEU")
    time.sleep(6)
    scroll(driver)
    
    results = []
    # 方法1：点所有「复制密码」按钮，密码会出现在页面或剪贴板
    try:
        # 先找「复制密码」按钮，点击后密码可能出现在input或span里
        btns = driver.find_elements(By.XPATH, "//button[contains(.,'复制密码')]|//button[contains(.,'查看密码')]|//button[contains(.,'显示密码')]")
        for btn in btns[:10]:
            try:
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.5)
            except Exception: pass
        time.sleep(1)
    except Exception: pass
    
    # 方法2：JS读取页面中存储密码的数据（Vue/React组件数据）
    try:
        data = driver.execute_script("""
            var out = [];
            // 找所有含密码的数据结构
            document.querySelectorAll('[class]').forEach(function(card){
                var t = card.innerText || card.textContent || '';
                var em = t.match(/[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i);
                if(!em) return;
                // 密码可能在input value里
                var inp = card.querySelector('input');
                var pwd = inp ? (inp.value || '') : '';
                // 或者在span/div的文本里（点按钮后显示）
                if(!pwd || pwd.length<5){
                    var spans = card.querySelectorAll('span,p,div');
                    for(var i=0;i<spans.length;i++){
                        var sv = spans[i].innerText||'';
                        if(sv && sv.length>=5 && sv.length<=32 && !sv.includes('@') && !sv.match(/^20\\d\\d/)){
                            pwd = sv.trim(); break;
                        }
                    }
                }
                var mt = t.match(/上次检查[:：\\s]*(20\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d)/);
                var ms = t.match(/(正常|解锁成功)/);
                if(em && pwd && pwd.length>=5 && ms){
                    out.push({email:em[0], pwd:pwd, time:mt?mt[1]:'', status:ms?ms[0]:'正常'});
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
                                 "checked_at": d.get("time", ""), "country": "美国"})
    except Exception as ex:
        logger.warning(f"  ip_share JS: {ex}")
    
    if not results:
        results = from_inputs(driver) or generic_parse(driver)
    return dedup(results)
    




SITES = [
    {"name":"ccbaohe.com/appleID",       "fn":crawl_ccbaohe},
    {"name":"shadowrocket.best",         "fn":crawl_shadowrocket_best},
    {"name":"free.iosapp.icu",           "fn":crawl_free_iosapp_icu},
    {"name":"idfree.top",                "fn":crawl_idfree_top},
    {"name":"id.btvda.top",              "fn":crawl_id_btvda_top},
    {"name":"idshare001.me",             "fn":crawl_idshare001},
    {"name":"app.iosr.cn",               "fn":crawl_app_iosr_cn},
    {"name":"id.bocchi2b.top",           "fn":crawl_bocchi2b},
    {"name":"139.196.183.52",            "fn":crawl_ip_share},
    {"name":"tkbaohe.com",               "fn":crawl_tkbaohe},
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
                logger.error(f"  {site['name']}: {e}")
                pairs = []
            nc = 0
            for p in pairs:
                e = p.get("email","").strip().lower()
                pw = p.get("password","").strip()
                if not e or not pw or "@" not in e or len(pw)<4: continue
                if len(set(pw))<2: continue
                if e not in seen:
                    seen[e] = {"id":uid(e),"email":e,"password":pw,
                               "status":p.get("status","正常"),
                               "country":p.get("country","美国"),
                               "checked_at":p.get("checked_at",""),
                               "source":site["name"],
                               "updated_at":now_cst()}
                    nc += 1
            source_stats[site["name"]] = nc
            logger.info(f"  → 新增 {nc} 条（共 {len(seen)} 条）")
            time.sleep(2)
    finally:
        driver.quit()
        logger.info("浏览器已关闭")

    accounts = sorted(seen.values(),
                      key=lambda a: a.get("checked_at","") or a.get("updated_at",""),
                      reverse=True)
    return {
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M"),
        "total": len(accounts),
        "source_stats": source_stats,
        "accounts": accounts,
    }
    
if __name__ == "__main__":
    output_path = os.environ.get("OUTPUT_FILE","apple_ids.json")
    result = crawl_all()
    with open(output_path,"w",encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 完成！共输出 {result['total']} 条账号 → {output_path}")
