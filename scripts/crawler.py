#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 v8
6个站点，按固定顺序：
1. idshare001.me/goso.html  → 直接请求 API（/node/getid.php，密码1分钟更新）
2. idfree.top               → strategy_data_clipboard（id精确配对）
3. ccbaohe.com/appleID      → strategy_mailto_onclick（mailto解码邮箱 + onclick copy密码）
4. tkbaohe.com/Shadowrocket → strategy_mailto_onclick（同 ccbaohe 结构）
5. id.btvda.top             → 直接请求 API（appleapi.omofunz.com/api/data，返回list）
6. id.bocchi2b.top          → API拦截（fetch/XHR拦截，返回list）

解析策略：
- ccbaohe/tkbaohe：Cloudflare 保护邮箱（data-cfemail解码）+ onclick copy() 取密码
- btvda/bocchi2b/idshare001：Vue3 应用，数据从 API 接口拉取，直接解析JSON
  API格式：list [{username/email, password, status(int), country, time}]

邮箱白名单：icloud/gmail/outlook/hotmail/qq/163/yahoo/proton/email.com 等
去重：账号+密码都相同才去重；账号相同密码不同时静默保留先抓到的
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

# 自定义 logging formatter，时间显示北京时间
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 日志时间显示北京时间
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

# ── 严格邮箱域名白名单 ──────────────────────────────
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
COUNTRY_JS = "美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|俄罗斯|巴西|墨西哥|阿根廷|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|荷兰|瑞典|波兰|乌克兰|中国大陆|蒙古"

TIME_RE = re.compile(r"(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)")

STATUS_BAD = {"异常", "不可用", "失效", "已失效", "locked", "invalid"}


# ══════════════════════════════════════════
# 基础工具
# ══════════════════════════════════════════

def is_valid_email(email: str) -> bool:
    """严格验证邮箱：本地部分≥4字符，域名在白名单内"""
    if not email or "@" not in email:
        return False
    parts = email.lower().split("@")
    if len(parts) != 2:
        return False
    local, domain = parts
    if len(local) < 4:          # 过滤遮蔽版（d@xxx）
        return False
    if domain not in VALID_DOMAINS:
        return False
    return True


def uid(email):
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]


def bad(status):
    return any(k in (status or "").lower() for k in STATUS_BAD)


def now_cst():
    return datetime.now(CST).isoformat()


def decode_cfemail(encoded: str) -> str:
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""


def find_country(text: str) -> str:
    m = COUNTRY_RE.search(text or "")
    return m.group(1) if m else ""


def find_time(text: str) -> str:
    m = TIME_RE.search(text or "")
    return m.group(1).strip() if m else ""


def dedup(lst):
    seen, out = set(), []
    for r in lst:
        e = r.get("email", "").lower().strip()
        if e and e not in seen and is_valid_email(e):
            seen.add(e)
            out.append(r)
    return out


def parse_text(text):
    """文本中直接提取 email+密码 对"""
    results, seen = [], set()
    INLINE = re.compile(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
        r"[\s\t]*(?:密码|password|pwd)?[\s::/|｜,，\t ]*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})", re.IGNORECASE)
    CTX_PWD = re.compile(
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*"
        r"([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})", re.IGNORECASE)
    for m in INLINE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if not is_valid_email(e):
            continue
        if (e, p) not in seen and len(p) >= 5:
            seen.add((e, p))
            results.append({"email": e, "password": p, "status": "正常",
                             "checked_at": "", "country": ""})
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m_e = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", line, re.I)
        if not m_e:
            continue
        e = m_e.group(0).lower()
        if not is_valid_email(e):
            continue
        ctx = "\n".join(lines[max(0, i-2):i+5])
        m_p = CTX_PWD.search(ctx)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        if m_p:
            k = (e, m_p.group(1).strip())
            if k not in seen and len(k[1]) >= 5:
                seen.add(k)
                results.append({"email": k[0], "password": k[1], "status": "正常",
                                 "checked_at": mt.group(1) if mt else "", "country": ""})
    return results


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
        "//div[contains(@class,'overlay')]//button",
        "//div[contains(@class,'mask')]//button",
        "//*[@aria-label='Close']",
        "//*[contains(@class,'close-btn')]",
        "//*[contains(@class,'modal-close')]",
    ]
    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.5)
        except Exception:
            pass


def fetch_html(url: str, timeout: int = 12) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.encoding = "utf-8"
        return resp.text if resp.status_code == 200 else ""
    except Exception:
        return ""


# ══════════════════════════════════════════
# 剪贴板钩子（idshare001 专用）
# ══════════════════════════════════════════

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
try {
    var _execOrig = document.execCommand.bind(document);
    document.execCommand = function(cmd) {
        if(cmd === 'copy') {
            try {
                var sel = window.getSelection();
                if(sel && sel.toString()) window.__copied.push(sel.toString());
            } catch(ex) {}
        }
        return _execOrig.apply(document, arguments);
    };
} catch(e) {}
"""


def click_all_copy_btns(driver, max_clicks=300):
    """点击所有复制按钮，拦截剪贴板，按顺序配对 email+password"""
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
    for i in range(min(len(emails), len(pwds))):
        e, p = emails[i], pwds[i]
        if is_valid_email(e) and p and e not in seen and len(p) >= 5:
            seen.add(e)
            results.append({"email": e, "password": p, "status": "正常",
                             "checked_at": "", "country": ""})
    # 策略2：整体 __copied 列表扫描配对
    if not results:
        copied = driver.execute_script("return window.__copied||[]")
        i = 0
        while i < len(copied) - 1:
            a, b = copied[i].strip(), copied[i+1].strip()
            if "@" in a and len(b) >= 5 and "@" not in b:
                e = a.lower()
                if is_valid_email(e) and e not in seen:
                    seen.add(e)
                    results.append({"email": e, "password": b, "status": "正常",
                                    "checked_at": "", "country": ""})
                i += 2
            else:
                i += 1
    return results


def enrich_country_time(driver, results):
    """从页面补充国家和时间"""
    if not results:
        return results
    emails_set = {r["email"] for r in results}
    try:
        js_data = driver.execute_script("""
var CPAT = new RegExp('(""" + COUNTRY_JS + r""")', 'u');
var TPAT = /(20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2})/;
var out = [], seen = {};
var emailSet = """ + json.dumps(list(emails_set)) + r""";
var emailPat = /[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}/i;
document.querySelectorAll('div,li,article,section,tr,td,p,span').forEach(function(el) {
    var t = (el.innerText || el.textContent || '').trim();
    if(t.length < 10 || t.length > 3000) return;
    var em = t.match(emailPat);
    if(!em) return;
    var e = em[0].toLowerCase();
    if(!emailSet.includes(e) || seen[e]) return;
    var ct = t.match(CPAT);
    var tm = t.match(TPAT);
    if(ct || tm) { seen[e]=1; out.push({email:e, country:ct?ct[1]:'', time:tm?tm[1]:''}); }
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
            if info[0]:
                r["country"] = info[0]
            if info[1] and not r.get("checked_at"):
                r["checked_at"] = info[1]
    except Exception as ex:
        logger.debug(f"enrich_country_time error: {ex}")
    return results


def from_inputs(driver):
    """从 input.value 提取"""
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
            if is_valid_email(e) and p and e not in seen and len(p) >= 5:
                seen.add(e)
                txt = d.get("txt", "")
                mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", txt)
                mc = COUNTRY_RE.search(txt)
                results.append({"email": e, "password": p, "status": "正常",
                                 "checked_at": mt.group(1) if mt else "",
                                 "country": mc.group(1) if mc else ""})
        return results
    except Exception:
        return []


def generic_parse(driver):
    """BeautifulSoup 通用解析"""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []
    seen = set()
    for card in soup.find_all(["div", "li", "article", "section", "tr"], recursive=True):
        text = card.get_text(" ", strip=True)
        if len(text) < 15:
            continue
        m_e = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}", text, re.I)
        if not m_e:
            continue
        e = m_e.group(0).lower()
        if not is_valid_email(e) or e in seen:
            continue
        m_p = re.search(r"密[码碼][\s:：]*([A-Za-z0-9!@#$%^&*()\-_=+]{5,32})", text)
        if not m_p:
            after = text[m_e.end():]
            m_p2 = re.search(r"\b([A-Za-z0-9!@#$%^&*\-_=+]{6,32})\b", after)
            if not m_p2:
                continue
            pwd = m_p2.group(1)
        else:
            pwd = m_p.group(1)
        m_s = re.search(r"(正常|可用|Normal|异常|不可用)", text, re.I)
        if m_s and bad(m_s.group(1)):
            continue
        m_t = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
        m_c = COUNTRY_RE.search(text)
        seen.add(e)
        results.append({"email": e, "password": pwd, "status": "正常",
                         "checked_at": m_t.group(1) if m_t else "",
                         "country": m_c.group(1) if m_c else ""})
    return dedup(results)


# ══════════════════════════════════════════
# 解析策略
# ══════════════════════════════════════════

def strategy_data_clipboard(html: str) -> list:
    """
    优先用 id 精确配对（username_N → password_N），
    其次用 .copy-btn / .copy-pass-btn，严格不跨卡片。
    """
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
        card_text = card.get_text(" ", strip=True) if card else ""
        country = ""
        if card:
            for anc in card.parents:
                country = find_country(anc.get_text(" ", strip=True)[:300])
                if country:
                    break
        seen.add(email)
        results.append({"email": email, "password": pw, "status": "正常",
                         "checked_at": find_time(card_text), "country": country})
    if results:
        return results

    # 方法2：逐卡片，.copy-btn / .copy-pass-btn，严格不跨卡
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
        card_text = card.get_text(" ", strip=True)
        country = ""
        for anc in card.parents:
            country = find_country(anc.get_text(" ", strip=True)[:300])
            if country:
                break
        seen.add(email)
        results.append({"email": email, "password": pw, "status": "正常",
                         "checked_at": find_time(card_text), "country": country})
    return results


def strategy_mailto_onclick(html: str) -> list:
    """
    ccbaohe / tkbaohe 专用：
    邮箱从 <a href="mailto:xxx"> 或 data-cfemail 解码
    密码从 <button onclick="copy('xxx')">
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

        # 密码从 onclick copy('xxx')：扫描卡片内所有按钮，不过滤文字
        pw = ""
        email_found_order = None
        for idx, btn in enumerate(card.select("button")):
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
            # 这个按钮复制的是邮箱（跳过）
            if is_valid_email(val.lower()):
                email_found_order = idx
                continue
            # 密码按钮：不含@ 且不是邮箱
            if "@" not in val and 4 <= len(val) <= 64:
                # 确保密码按钮在邮箱按钮之后（或者卡片内只有一个密码按钮）
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

        results.append({"email": email.lower().strip(), "password": pw.strip(),
                         "status": "正常", "checked_at": checked_at, "country": country})
    return results


# ══════════════════════════════════════════
# 站点专属爬虫
# ══════════════════════════════════════════


# 拦截 fetch/XHR 的 JS 代码（页面加载前注入）
INTERCEPT_JS = r"""
window.__api_responses = window.__api_responses || [];
window.__api_all = window.__api_all || [];
// 拦截 fetch
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
// 拦截 XMLHttpRequest
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
    """
    等待 Vue 从 API 拉取数据，返回账号列表。
    同时打印所有 API 请求的诊断信息。
    """
    driver.execute_script(INTERCEPT_JS)
    
    deadline = time.time() + wait_secs
    while time.time() < deadline:
        time.sleep(0.5)
        all_calls = driver.execute_script("return window.__api_all || []")
        for call in all_calls:
            data = call.get("data")
            # 格式1：直接 list（idshare001/btvda）
            if isinstance(data, list) and len(data) > 0:
                first = data[0] if data else {}
                # 只要是包含字符串字段的dict列表就接受
                if isinstance(first, dict) and any(
                    isinstance(v, str) for v in first.values()
                ):
                    return data
            # 格式2：{id:[...]} 或 {accounts:[...]}
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
    
    # 超时：打印所有API请求诊断
    all_calls = driver.execute_script("return window.__api_all || []")
    if all_calls:
        logger.info(f"  {site_name} 拦截到 {len(all_calls)} 个API请求:")
        for call in all_calls[:5]:
            url = call.get("url","")[:80]
            data = call.get("data",{})
            keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
            logger.info(f"    URL={url} keys={keys}")
    else:
        logger.info(f"  {site_name} 没有拦截到任何API请求")
    return []


def _to_cst(ts: str) -> str:
    """API 返回的时间已经是北京时间，直接返回"""
    if not ts:
        return ""
    m = re.search(r"(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)", str(ts))
    return m.group(1).replace("T", " ") if m else str(ts)


def parse_vue_accounts(raw_list: list, site_name="") -> list:
    """把 API 返回的账号列表转换为标准格式"""
    results = []
    if not raw_list:
        return results
    # 诊断：打印第一条原始数据的字段
    if raw_list:
        first = raw_list[0]
        logger.info(f"  {site_name} API数据样本字段: {list(first.keys()) if isinstance(first, dict) else type(first)}")
        if isinstance(first, dict):
            logger.info(f"  {site_name} 第一条: {dict(list(first.items())[:6])}")
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        # 邮箱：兼容 email / username / account / user
        email = str(item.get("email") or item.get("username") or
                    item.get("account") or item.get("user") or "").strip().lower()
        # 密码
        pw = str(item.get("password") or item.get("pwd") or
                 item.get("pass") or item.get("passwd") or "").strip()
        # 处理 unicode 转义（如 \u0026 → &）
        try:
            if "\\u" in pw or "%u" in pw:
                pw = pw.encode("raw_unicode_escape").decode("unicode_escape")
        except Exception:
            pass
        # status：int 1=正常/0=异常，str "正常"/"异常"
        raw_status = item.get("status", 1)
        if isinstance(raw_status, int):
            status_ok = raw_status == 1
        else:
            status_ok = not bad(str(raw_status))
        # country：idshare001/btvda 的 country 字段有时存的是状态文字
        # 只有出现国家关键词才用，否则默认美国
        raw_country = str(item.get("country") or item.get("region") or item.get("area") or "")
        country = find_country(raw_country) or "美国"

        if not email or "@" not in email:
            continue
        if not pw:
            continue
        if not is_valid_email(email):
            continue
        if not status_ok:
            continue
        results.append({
            "email": email, "password": pw, "status": "正常",
            "checked_at": _to_cst(str(item.get("time") or item.get("checked_at") or item.get("update_time") or "")),
            "country": country,
        })
    return results

def crawl_idshare001(driver) -> list:
    """
    idshare001.me — Vue3 + Vite 应用，数据从 VITE_API_URL 接口拉取
    格式：{ "id": [{email, password, status, country}] }
    策略：
    1. 先注入 fetch/XHR 拦截器
    2. 点击"我是老玩家"通过弹窗
    3. 等待 API 响应数据
    4. 从拦截到的 JSON 直接提取账号
    """
    urls = ["https://idshare001.me/goso.html", "https://idshare001.me/"]
    loaded = False
    for url in urls:
        try:
            # 先注入拦截器，再加载页面
            driver.get("about:blank")
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
                {"source": INTERCEPT_JS})
            driver.get(url)
            WebDriverWait(driver, 12).until(
                lambda d: d.execute_script("return document.readyState") == "complete")
            if len(driver.page_source) > 2000:
                loaded = True
                logger.info(f"  idshare001 有效URL: {url}")
                break
        except Exception:
            continue

    if not loaded:
        logger.info("  idshare001 抓到: 0")
        return []

    time.sleep(1)
    # 点击"我是老玩家"
    for xpath in ["//button[contains(.,'我是老玩家')]", "//button[contains(.,'老玩家')]"]:
        try:
            btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", btn)
            logger.info(f"  idshare001 点击: {btn.text.strip()}")
            time.sleep(1)
            break
        except Exception:
            pass

    # 先直接请求已知API（/node/getid.php?getid=1 和 getid=2，返回直接list）
    raw = []
    for api_path in ["/node/getid.php?getid=2", "/node/getid.php?getid=1"]:
        try:
            resp = requests.get("https://idshare001.me" + api_path, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    raw.extend(data)
                    logger.info(f"  idshare001 direct API {api_path} → {len(data)} 条")
        except Exception as ex:
            logger.debug(f"  idshare001 direct API {api_path}: {ex}")

    if not raw:
        # 兜底：Selenium 拦截
        raw = extract_from_vue_api(driver, wait_secs=15, site_name="idshare001")

    logger.info(f"  idshare001 API拦截到 {len(raw)} 条原始数据")
    results = parse_vue_accounts(raw, "idshare001")
    logger.info(f"  idshare001 抓到: {len(results)}")
    return dedup(results)


def crawl_idfree_top(driver) -> list:
    """idfree.top — strategy_data_clipboard（id精确配对）"""
    loaded = False
    for url in ["https://idfree.top/", "https://www.idfree.top/"]:
        try:
            driver.get(url)
            WebDriverWait(driver, 12).until(
                lambda d: d.execute_script("return document.readyState") == "complete")
            if "@" in driver.page_source and len(driver.page_source) > 2000:
                loaded = True
                break
        except Exception:
            continue

    if not loaded:
        html = fetch_html("https://idfree.top/")
        if html and "@" in html:
            r = strategy_data_clipboard(html) or parse_text(html)
            if r:
                logger.info(f"  idfree_top [requests] → {len(r)} 条")
                return dedup(r)
        logger.info("  idfree_top 抓到: 0")
        return []

    time.sleep(2)
    # idfree 有"我已阅读，继续查看账号"弹窗，必须点击才能显示账号
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
            logger.info(f"  idfree 点击入口按钮: {btn.text.strip()}")
            time.sleep(2)
            break
        except Exception:
            pass
    close_popups(driver)
    scroll(driver, n=10)
    time.sleep(2)

    # idfree 图四：账号在 input 里，有复制按钮，用 strategy_data_clipboard 或 click_card_by_card
    results = strategy_data_clipboard(driver.page_source)
    if not results:
        results = click_card_by_card(driver, ".btn-copy-account", ".btn-copy-password")
    if not results:
        results = click_all_copy_btns(driver)
        results = enrich_country_time(driver, results)
    if not results:
        results = generic_parse(driver)

    logger.info(f"  idfree_top 抓到: {len(results)}")
    return dedup(results)


def crawl_ccbaohe(driver) -> list:
    """ccbaohe.com — strategy_mailto_onclick"""
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
    """tkbaohe.com — strategy_mailto_onclick（同 ccbaohe 结构）"""
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



def click_card_by_card(driver, account_cls, password_cls) -> list:
    """
    逐卡片点击：先点本卡片的账号按钮拦截邮箱，再点密码按钮拦截密码。
    专为 btvda/bocchi2b 设计（btn-copy-account / btn-copy-password）。
    """
    driver.execute_script(HOOK_JS)
    time.sleep(0.5)

    # 找所有账号按钮
    acct_btns = driver.find_elements(By.CSS_SELECTOR, account_cls)
    results = []
    seen = set()

    for acct_btn in acct_btns:
        try:
            # 重置已拦截列表，只看本次点击
            before = driver.execute_script("var n=window.__copied.length; return n;")

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", acct_btn)
            driver.execute_script("arguments[0].click();", acct_btn)
            time.sleep(0.15)

            copied = driver.execute_script("return window.__copied||[]")
            if len(copied) <= before:
                continue
            email_val = copied[-1].strip().lower()
            if not is_valid_email(email_val):
                continue

            # 找同一个卡片容器下的密码按钮
            # 向上最多查找10层父元素，找到包含密码按钮的最小容器
            pw_btn = driver.execute_script("""
var btn = arguments[0];
var pwSel = arguments[1];
// 先从直接父元素往上找
var el = btn.parentElement;
for(var i=0; i<10; i++) {
    if(!el || el === document.body) break;
    var pwBtn = el.querySelector(pwSel);
    if(pwBtn && pwBtn !== btn) return pwBtn;
    el = el.parentElement;
}
return null;
            """, acct_btn, password_cls)

            if not pw_btn:
                continue

            before2 = driver.execute_script("return window.__copied.length;")
            driver.execute_script("arguments[0].click();", pw_btn)
            time.sleep(0.15)

            copied2 = driver.execute_script("return window.__copied||[]")
            if len(copied2) <= before2:
                continue
            pw_val = copied2[-1].strip()
            if not pw_val or "@" in pw_val or len(pw_val) < 4:
                continue

            if email_val not in seen:
                seen.add(email_val)
                results.append({"email": email_val, "password": pw_val,
                                 "status": "正常", "checked_at": "", "country": ""})
        except Exception:
            continue

    return results

def crawl_id_btvda_top(driver) -> list:
    """
    id.btvda.top — API: https://appleapi.omofunz.com/api/data（返回直接list）
    """
    # 直接请求已知API
    try:
        resp = requests.get("https://appleapi.omofunz.com/api/data",
                            headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            raw = resp.json()
            if isinstance(raw, list) and len(raw) > 0:
                results = parse_vue_accounts(raw)
                if results:
                    logger.info(f"  id.btvda.top [direct API] → {len(results)} 条")
                    return dedup(results)
    except Exception as ex:
        logger.debug(f"  btvda direct API: {ex}")

    # 兜底：Selenium + 拦截
    url = "https://id.btvda.top/"
    try:
        driver.get("about:blank")
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
            {"source": INTERCEPT_JS})
        driver.get(url)
        time.sleep(4)
        close_popups(driver)

        raw = extract_from_vue_api(driver, wait_secs=15, site_name="btvda")
        logger.info(f"  btvda API拦截到 {len(raw)} 条原始数据")
        results = parse_vue_accounts(raw, "btvda")
        logger.info(f"  id.btvda.top 抓到: {len(results)}")
        return dedup(results)
    except Exception as ex:
        logger.error(f"  id.btvda.top error: {ex}")
        return []


def crawl_bocchi2b(driver) -> list:
    """
    id.bocchi2b.top — 同框架，先尝试 API 拦截，再用静态 onclick 解析
    """
    url = "https://id.bocchi2b.top/"

    def parse_onclick(html):
        """静态解析 onclick=copyToClipboard(...)"""
        soup = BeautifulSoup(html, "lxml")
        results = []
        btns = soup.find_all("button", onclick=True)
        i = 0
        while i < len(btns) - 1:
            a_m = re.search(r"copyToClipboard\('([^']+)'\)", btns[i].get("onclick",""))
            b_m = re.search(r"copyToClipboard\('([^']+)'\)", btns[i+1].get("onclick",""))
            if a_m and b_m:
                email = a_m.group(1).lower().strip()
                pw = b_m.group(1).strip()
                if is_valid_email(email) and pw and "@" not in pw:
                    results.append({"email": email, "password": pw,
                                     "status": "正常", "checked_at": "", "country": "美国"})
                    i += 2; continue
            i += 1
        return results

    # 1. requests 静态解析
    html = fetch_html(url)
    if html:
        r = parse_onclick(html)
        if r:
            logger.info(f"  bocchi2b [requests静态] → {len(r)} 条")
            return dedup(r)

    # 2. Selenium + API 拦截
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
            logger.info(f"  bocchi2b API拦截到 {len(raw)} 条")
            results = parse_vue_accounts(raw)
            logger.info(f"  bocchi2b [API] → {len(results)} 条")
            return dedup(results)

        # 3. Selenium page_source 静态解析兜底
        r = parse_onclick(driver.page_source)
        logger.info(f"  bocchi2b [selenium静态] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  bocchi2b error: {ex}")
        return []


# ══════════════════════════════════════════
# 站点配置（固定顺序）
# ══════════════════════════════════════════

SITES = [
    {"name": "idshare001.me",       "fn": crawl_idshare001},
    {"name": "idfree.top",          "fn": crawl_idfree_top},
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
    records = {}
    source_stats = {}

    logger.info("启动浏览器...")
    driver = make_driver()
    try:
        for site in SITES:
            logger.info(f"▶ 抓取: {site['name']}")
            try:
                pairs = site["fn"](driver)
            except Exception as ex:
                logger.error(f"  {site['name']} 异常: {ex}")
                pairs = []

            nc = 0
            for p in pairs:
                e = p.get("email", "").strip().lower()
                pw = p.get("password", "").strip()

                # 严格过滤
                if not is_valid_email(e):
                    continue
                if not pw or len(pw) < 4 or len(pw) > 64:
                    continue
                if len(set(pw)) < 2:
                    continue
                # 过滤 HTML 实体（&amp; 说明解析层没处理好）
                if "&" in pw and "amp;" in pw:
                    pw = pw.replace("&amp;", "&").replace("&lt;", "<").replace(
                        "&gt;", ">").replace("&quot;", '"').replace("&#39;", "'")

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
                    # 同一次运行内，同一邮箱出现多次：保留先抓到的站点的密码
                    # （优先级按 SITES 顺序，先抓到的更可信）
                    existing = records[e]
                    # 补充国家/时间（如果之前没有）
                    if p.get("country") and not existing.get("country"):
                        existing["country"] = p["country"]
                    new_t = p.get("checked_at", "")
                    old_t = existing.get("checked_at", "")
                    if new_t and new_t > old_t:
                        existing["checked_at"] = new_t

            source_stats[site["name"]] = nc
            logger.info(f"  → 新增 {nc} 条（共 {len(records)} 条）"
                        f"  [抓到 {len(pairs)} 条，重复/冲突 {len(pairs)-nc} 条]")
            time.sleep(1)
    finally:
        driver.quit()
        logger.info("浏览器已关闭")

    def sort_key(a):
        return (SITE_ORDER.get(a.get("source", ""), 999),
                a.get("checked_at", "") or a.get("updated_at", "") or "")

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
