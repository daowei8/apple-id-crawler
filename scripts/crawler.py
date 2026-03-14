#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 v6
策略：
- btvda/bocchi2b/idshare001/idfree/shadowrocket/free.iosapp/app.iosr → click_all_copy_btns（剪贴板钩子）+ 修复版 enrich_country_time
- ccbaohe/tkbaohe → strategy_mailto_onclick（mailto href + onclick copy()）
- 139.196.183.52 → strategy_data_clipboard（data-clipboard-text）
- 各站点都有多层兜底
- 国家/时间精确提取，不再默认美国
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
EMAIL_BROAD = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}"

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
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""


def find_country(text: str) -> str:
    if not text:
        return ""
    m = COUNTRY_RE.search(text)
    return m.group(1) if m else ""


def find_time(text: str) -> str:
    m = TIME_RE.search(text)
    return m.group(1).strip() if m else ""


def dedup(lst):
    seen, out = set(), []
    for r in lst:
        e = r.get("email", "").lower().strip()
        if e and e not in seen:
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
        r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:.]{6,32})",
        re.IGNORECASE)
    for m in INLINE.finditer(text):
        e, p = m.group(1).lower(), m.group(2)
        if (e, p) not in seen and len(p) >= 5:
            seen.add((e, p))
            results.append({"email": e, "password": p, "status": "正常", "checked_at": "", "country": ""})
    lines = text.splitlines()
    for i, line in enumerate(lines):
        emails = EMAIL_RE.findall(line)
        if not emails:
            continue
        ctx = "\n".join(lines[max(0, i-2):i+5])
        m = CTX_PWD.search(ctx)
        mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", ctx)
        if m:
            for e in emails:
                k = (e.lower(), m.group(1).strip())
                if k not in seen and len(k[1]) >= 5:
                    seen.add(k)
                    results.append({"email": k[0], "password": k[1],
                                    "status": "正常", "checked_at": mt.group(1) if mt else "", "country": ""})
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
    opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
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
        "//button[contains(.,'close')]",
        "//button[text()='Ok']",
        "//button[text()='OK']",
        "//a[contains(.,'我知道了')]",
        "//a[contains(.,'知道了')]",
        "//div[contains(@class,'modal')]//button",
        "//div[contains(@class,'dialog')]//button",
        "//div[contains(@class,'popup')]//button",
        "//div[contains(@class,'notice')]//button",
        "//div[contains(@class,'overlay')]//button",
        "//div[contains(@class,'mask')]//button",
        "//*[@aria-label='Close']",
        "//*[@aria-label='close']",
        "//*[contains(@class,'close-btn')]",
        "//*[contains(@class,'closeBtn')]",
        "//*[contains(@class,'modal-close')]",
    ]
    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.XPATH, sel)))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.6)
        except Exception:
            pass


# ══════════════════════════════════════════
# 剪贴板钩子方法（btvda/bocchi2b等专用）
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


def click_all_copy_btns(driver, max_clicks=400):
    """点击所有复制按钮，拦截剪贴板内容，按顺序配对"""
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
    for i in range(len(emails)):
        e = emails[i]
        p = pwds[i] if i < len(pwds) else ""
        if e and p and e not in seen and len(p) >= 5:
            seen.add(e)
            results.append({"email": e, "password": p, "status": "正常", "checked_at": "", "country": ""})

    # 策略2：__copied 列表整体扫描配对
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
                                    "checked_at": "", "country": ""})
                i += 2
            else:
                i += 1

    return results


def enrich_country_time(driver, results):
    """
    从页面 JS 补充每个账号的国家和时间。
    修复：不限制 t.length > 1000，改为找最小包含目标邮箱的元素。
    """
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

// 遍历所有叶子附近的元素，找包含目标邮箱的最小元素
document.querySelectorAll('div,li,article,section,tr,td,p,span').forEach(function(el) {
    var t = (el.innerText || el.textContent || '').trim();
    if(t.length < 10 || t.length > 3000) return;
    var em = t.match(emailPat);
    if(!em) return;
    var e = em[0].toLowerCase();
    if(!emailSet.includes(e) || seen[e]) return;
    var ct = t.match(CPAT);
    var tm = t.match(TPAT);
    if(ct || tm) {
        seen[e] = 1;
        out.push({email: e, country: ct ? ct[1] : '', time: tm ? tm[1] : ''});
    }
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


def js_full_scan(driver):
    """JS全量扫描：data-clipboard-text + onclick copy() + 密码关键词"""
    try:
        raw = driver.execute_script("""
var results = [];
var seen = {};
var EMAIL_P = /[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[a-z]{2,}/i;
var COUNTRY_P = /(美国|英国|日本|香港|台湾|韩国|越南|澳大利亚|新加坡|加拿大|德国|法国|土耳其|俄罗斯|巴西|印度|泰国|马来西亚|菲律宾|印尼|意大利|西班牙|荷兰|蒙古|中国大陆)/;
var TIME_P = /(20\\d{2}-\\d{2}-\\d{2}[\\sT]\\d{2}:\\d{2}(?::\\d{2})?)/;
var BAD_P = /异常|失效|不可用|锁定/;
var PWD_P = /密[码碼][\\s:：]*([A-Za-z0-9!@#$%^&*()\\-_=+]{5,32})/;

var containers = Array.from(document.querySelectorAll(
    '.card-body,.card,.item,.id-item,.account-item'
));
if(containers.length < 2) {
    containers = Array.from(document.querySelectorAll('div,li,article')).filter(function(el) {
        var t = el.innerText || '';
        return EMAIL_P.test(t) && t.length >= 20 && t.length < 2000;
    });
}

containers.forEach(function(card) {
    var text = (card.innerText || card.textContent || '').trim();
    if(!text || text.length > 2000 || text.length < 15) return;
    var emailMatch = text.match(EMAIL_P);
    if(!emailMatch) return;
    var email = emailMatch[0].toLowerCase();
    if(seen[email]) return;
    if(BAD_P.test(text)) return;

    var pwd = '';

    // 1. data-clipboard-text（不含@）
    card.querySelectorAll('[data-clipboard-text]').forEach(function(el) {
        if(pwd) return;
        var v = (el.getAttribute('data-clipboard-text') || '').trim();
        if(v && v.indexOf('@') < 0 && v.length >= 4 && v.length <= 64) pwd = v;
    });

    // 2. onclick copy()
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

    // 3. 密码关键词
    if(!pwd) {
        var pm = text.match(PWD_P);
        if(pm) pwd = pm[1];
    }

    // 4. input value
    if(!pwd) {
        card.querySelectorAll('input').forEach(function(inp) {
            if(pwd) return;
            var v = (inp.value || inp.getAttribute('value') || '').trim();
            if(v && v.indexOf('@') < 0 && v.length >= 4 && v.length <= 64) pwd = v;
        });
    }

    if(!pwd || pwd.length < 4) return;

    var country = '';
    var el = card;
    for(var i=0; i<6; i++) {
        el = el.parentElement;
        if(!el) break;
        var cm = (el.innerText||'').match(COUNTRY_P);
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
        out = []
        seen = set()
        for d in (raw or []):
            e = (d.get("email") or "").lower().strip()
            p = (d.get("pwd") or "").strip()
            if e and p and "@" in e and 4 <= len(p) <= 64 and e not in seen:
                seen.add(e)
                out.append({"email": e, "password": p, "status": "正常",
                             "checked_at": (d.get("time") or "").strip(),
                             "country": d.get("country") or ""})
        return out
    except Exception as ex:
        logger.debug(f"js_full_scan error: {ex}")
        return []


def from_inputs(driver):
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
            if e and p and "@" in e and e not in seen and len(p) >= 5:
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


def fetch_html(url: str, timeout: int = 12) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.encoding = "utf-8"
        return resp.text if resp.status_code == 200 else ""
    except Exception:
        return ""


# ══════════════════════════════════════════
# 专用静态解析策略
# ══════════════════════════════════════════

def strategy_data_clipboard(html: str) -> list:
    """
    data-clipboard-text 按钮解析。
    配对策略（按优先级）：
    1. id 精确配对：username_N 对应 password_N（最可靠）
    2. 类名配对：.copy-btn（邮箱）对应 .copy-pass-btn（密码）
    3. 按钮颜色：btn-primary（邮箱）对应 btn-success（密码）
    绝不跨卡片 fallback 取值
    """
    soup = BeautifulSoup(html, "lxml")
    results = []

    # 方法1：直接用 id 精确配对（username_N → password_N）
    seen = set()
    for btn in soup.select("button[id^='username_'], a[id^='username_']"):
        uid_n = btn.get("id", "")[9:]  # "username_43" → "43"
        email = btn.get("data-clipboard-text", "").strip().lower()
        if not email or "@" not in email or email in seen:
            continue
        pw_btn = soup.select_one(f"#password_{uid_n}")
        if not pw_btn:
            continue
        pw = pw_btn.get("data-clipboard-text", "").strip()
        if not pw or "@" in pw or len(pw) < 4:
            continue
        # 找该按钮所在的 card-body 取时间/国家
        card = btn.find_parent(class_="card-body") or btn.find_parent(class_="card")
        card_text = card.get_text(" ", strip=True) if card else ""
        country = ""
        if card:
            for anc in card.parents:
                country = find_country(anc.get_text(" ", strip=True)[:300])
                if country:
                    break
        seen.add(email)
        results.append({
            "email": email,
            "password": pw,
            "status": "正常",
            "checked_at": find_time(card_text),
            "country": country,
        })

    if results:
        return results

    # 方法2：逐卡片解析（.card-body 为边界，严格不跨卡）
    cards = soup.select(".card-body")
    if not cards:
        # 找所有包含 data-clipboard-text 按钮的最小公共父元素
        seen_parents = []
        for btn in soup.select("[data-clipboard-text]"):
            p = btn.find_parent(class_=lambda c: c and any(
                k in c for k in ("col-", "card", "item", "account")))
            if p and p not in seen_parents:
                seen_parents.append(p)
        cards = seen_parents if seen_parents else []

    for card in cards:
        # 严格：只在本卡片内找邮箱按钮和密码按钮，不跨卡
        email = ""
        email_btn = None
        for sel in [".copy-btn",
                    "button.btn-primary[data-clipboard-text]",
                    "a.btn-primary[data-clipboard-text]"]:
            b = card.select_one(sel)
            if b:
                v = b.get("data-clipboard-text", "").strip().lower()
                if v and "@" in v:
                    email = v
                    email_btn = b
                    break

        if not email or "@" not in email or email in seen:
            continue

        password = ""
        # 密码按钮：必须和邮箱按钮在同一个卡片内
        for sel in [".copy-pass-btn",
                    "button.btn-success[data-clipboard-text]",
                    "a.btn-success[data-clipboard-text]"]:
            b = card.select_one(sel)
            if b:
                v = b.get("data-clipboard-text", "").strip()
                if v and "@" not in v and 4 <= len(v) <= 64:
                    password = v
                    break

        if not password:
            continue

        card_text = card.get_text(" ", strip=True)
        badge = card.select_one(".badge")
        if badge and bad(badge.get_text(strip=True)):
            continue

        country = ""
        for anc in card.parents:
            country = find_country(anc.get_text(" ", strip=True)[:300])
            if country:
                break

        seen.add(email)
        results.append({
            "email": email,
            "password": password,
            "status": "正常",
            "checked_at": find_time(card_text),
            "country": country,
        })
    return results


def strategy_mailto_onclick(html: str) -> list:
    """
    mailto href 邮箱 + onclick copy('密码')
    ccbaohe.com / tkbaohe.com 专用
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
            for btn in card.select("[data-clipboard-text]"):
                v = btn.get("data-clipboard-text", "").strip().lower()
                if "@" in v:
                    email = v
                    break
        if not email or "@" not in email:
            continue

        # 密码：onclick copy()
        password = ""
        for btn in card.select("button"):
            btn_text = btn.get_text(strip=True)
            if "密码" not in btn_text and "copy" not in btn_text.lower():
                continue
            oc = btn.get("onclick", "")
            m = (re.search(r"copy\('([^']{4,64})'\)", oc) or
                 re.search(r'copy\("([^"]{4,64})"\)', oc) or
                 re.search(r"copy\(&#39;([^&]{4,64})&#39;\)", oc) or
                 re.search(r"copy\(([A-Za-z0-9!@#$%^&*()\-_=+]{4,64})\)", oc))
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

        # 国家：优先 card-header
        country = ""
        header = card.find_previous("div", class_="card-header")
        if header:
            country = find_country(header.get_text())
        if not country:
            country = find_country(card_text)

        mt = re.search(r"检测时间[：:\s]*(20\d{2}-\d{2}-\d{2}\s\d{2}:\d{2}(?::\d{2})?)", card_text)
        checked_at = mt.group(1) if mt else find_time(card_text)

        results.append({
            "email": email.lower().strip(),
            "password": password.strip(),
            "status": "正常",
            "checked_at": checked_at,
            "country": country,
        })
    return results


def strategy_plaintext(html: str) -> list:
    """纯文本格式（free.iosapp.icu）"""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)
    results = []
    seen = set()

    blocks = re.split(r"(?=账[号号][:：])", text, flags=re.IGNORECASE)
    if len(blocks) <= 1:
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
        results.append({
            "email": email,
            "password": password,
            "status": "正常",
            "checked_at": checked_at,
            "country": find_country(block),
        })
    return results


# ══════════════════════════════════════════
# 各站点专属爬虫
# ══════════════════════════════════════════

def crawl_idshare001(driver) -> list:
    """
    idshare001.me — Cloudflare 保护，加载后会被JS挑战替换
    策略：readyState=complete 后立刻解析，不等 CF 替换页面
    有效URL判断：页面 >5000 字节 且 含真实邮箱（至少3个字符本地部分）
    """
    urls = [
        "https://idshare001.me/goso.html",
        "https://idshare001.me/",
        "https://idshare001.me/apple",
        "https://idshare001.me/free",
        "https://idshare001.me/share",
    ]

    EMAIL_CHECK = re.compile(r'[A-Za-z0-9._%+\-]{4,}@[A-Za-z0-9.\-]+\.[a-z]{2,}', re.I)

    for url in urls:
        try:
            driver.get(url)
            # 等 readyState=complete，但最多等12秒
            WebDriverWait(driver, 12).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            src_page = driver.page_source
            # 严格判断：必须 >5000 字节 且 有完整邮箱（防止 CF 挑战页混进来）
            real_emails = EMAIL_CHECK.findall(src_page)
            if len(src_page) > 5000 and len(real_emails) >= 1:
                logger.info(f"  idshare001 有效URL: {url} (邮箱={len(real_emails)}, 页面={len(src_page)}字节)")
                # ★ 立刻解析，不等 CF 替换页面
                results = []
                # 先试静态解析（此时页面还是真实内容）
                r = strategy_data_clipboard(src_page) or parse_text(src_page)
                if r:
                    logger.info(f"  idshare001 [立即静态解析] → {len(r)} 条")
                    results = r
                # 再等一下做 Selenium 解析（也许 CF 没替换）
                if not results:
                    time.sleep(1)
                    close_popups(driver)
                    scroll(driver, n=8)
                    time.sleep(1)
                    # 检查页面是否还在
                    check = driver.page_source
                    if len(check) > 3000 and EMAIL_CHECK.search(check):
                        results = click_all_copy_btns(driver)
                        results = enrich_country_time(driver, results)
                        if not results:
                            results = js_full_scan(driver)
                        if not results:
                            results = from_inputs(driver)
                        if results:
                            logger.info(f"  idshare001 [Selenium后解析] → {len(results)} 条")
                    else:
                        logger.warning(f"  idshare001 页面已被CF替换 (现在={len(check)}字节)")
                        # CF替换了，用刚才保存的静态HTML再试
                        results = strategy_data_clipboard(src_page) or parse_text(src_page)
                if results:
                    logger.info(f"  idshare001 抓到: {len(results)}")
                    return dedup(results)
        except Exception as ex:
            logger.debug(f"  idshare001 {url}: {ex}")
            continue

    # 所有 Selenium 路径失败，用 requests 直接拉
    logger.warning("  idshare001 所有Selenium路径失败，尝试requests")
    for url in urls:
        html = fetch_html(url)
        if html and len(html) > 5000 and EMAIL_CHECK.search(html):
            r = strategy_data_clipboard(html) or parse_text(html)
            if r:
                logger.info(f"  idshare001 [requests] → {len(r)} 条")
                return dedup(r)

    logger.info("  idshare001 抓到: 0")
    return []


def crawl_idfree_top(driver) -> list:
    """idfree.top"""
    loaded = False
    for url in ["https://idfree.top/", "https://www.idfree.top/", "https://idfree.top/free"]:
        try:
            driver.get(url)
            WebDriverWait(driver, 12).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            if "@" in driver.page_source and len(driver.page_source) > 2000:
                loaded = True
                break
        except Exception:
            continue

    if not loaded:
        for url in ["https://idfree.top/", "https://www.idfree.top/"]:
            html = fetch_html(url)
            if html and "@" in html:
                r = strategy_data_clipboard(html) or parse_text(html)
                if r:
                    logger.info(f"  idfree_top [requests] → {len(r)} 条")
                    return dedup(r)
        logger.info("  idfree_top 抓到: 0")
        return []

    time.sleep(2)
    close_popups(driver)
    scroll(driver, n=10)
    time.sleep(2)

    # 先读 data-clipboard-text 属性（最准确，不依赖剪贴板事件）
    results = strategy_data_clipboard(driver.page_source)
    if not results:
        results = click_all_copy_btns(driver)
        results = enrich_country_time(driver, results)
    if not results:
        results = js_full_scan(driver)
    if not results:
        results = from_inputs(driver)

    logger.info(f"  idfree_top 抓到: {len(results)}")
    return dedup(results)


def crawl_ip_share(driver) -> list:
    """139.196.183.52 — data-clipboard-text"""
    base = "http://139.196.183.52"
    discovered = {f"{base}/share/DZhBvnglEU"}
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
        # requests 失败，Selenium
        try:
            driver.get(url)
            time.sleep(5)
            close_popups(driver)
            scroll(driver, n=6)
            r = strategy_data_clipboard(driver.page_source)
            if not r:
                r = click_all_copy_btns(driver)
                r = enrich_country_time(driver, r)
            if not r:
                r = js_full_scan(driver)
            if r:
                logger.info(f"    [selenium] {url} → {len(r)} 条")
            all_results.extend(r)
        except Exception as ex:
            logger.error(f"  ip_share error {url}: {ex}")

    logger.info(f"  139.196.183.52 抓到: {len(dedup(all_results))}")
    return dedup(all_results)


def crawl_free_iosapp_icu(driver) -> list:
    """free.iosapp.icu — 纯文本 + 剪贴板两种方式"""
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

        # 先试纯文本解析
        r = strategy_plaintext(driver.page_source)
        if r:
            logger.info(f"  free.iosapp.icu [static] → {len(r)} 条")
            return dedup(r)

        # 再试剪贴板
        r = click_all_copy_btns(driver)
        r = enrich_country_time(driver, r)
        if not r:
            r = js_full_scan(driver)
        if not r:
            r = from_inputs(driver)
        logger.info(f"  free.iosapp.icu [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  free.iosapp.icu error: {ex}")
        return []


def crawl_app_iosr_cn(driver) -> list:
    """app.iosr.cn — 必须 Selenium，点刷新，全套解析"""
    try:
        driver.get("https://app.iosr.cn/tools/apple-shared-id")
        time.sleep(7)
        close_popups(driver)
        try:
            driver.find_element(By.XPATH, "//button[contains(.,'刷新')]").click()
            time.sleep(4)
        except Exception:
            pass
        scroll(driver, n=8)
        time.sleep(2)

        # 先滚动确保所有卡片渲染
        scroll(driver, n=10)
        time.sleep(1)
        # 剪贴板方式
        r = click_all_copy_btns(driver, max_clicks=500)
        r = enrich_country_time(driver, r)
        if r:
            logger.info(f"  app.iosr.cn [clipboard] → {len(r)} 条")
            return dedup(r)

        # JS全量扫描
        r = js_full_scan(driver)
        if r:
            logger.info(f"  app.iosr.cn [js_scan] → {len(r)} 条")
            return dedup(r)

        # BeautifulSoup 补充逻辑
        seen = set()
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for card in soup.find_all(["div", "li", "article"], class_=True):
            text = card.get_text(" ", strip=True)
            if len(text) < 20 or len(text) > 600:
                continue
            me = EMAIL_RE.search(text)
            if not me:
                continue
            e = me.group().lower()
            if e in seen:
                continue
            mp = re.search(r"密[码碼][\s:：]*([^\s]{5,32})", text)
            if not mp:
                after = text[me.end():]
                mp2 = re.search(r"\b([A-Za-z0-9!@#$%^&*()\-_=+:]{6,32})\b", after)
                if not mp2:
                    continue
                pwd = mp2.group(1)
            else:
                pwd = mp.group(1)
            if re.match(r"^20\d\d[-/]\d\d[-/]\d\d", pwd):
                continue
            mt = re.search(r"(20\d\d-\d\d-\d\d \d\d:\d\d)", text)
            ms = re.search(r"(正常使用|正常|可用)", text)
            if not ms:
                continue
            mc = COUNTRY_RE.search(text)
            seen.add(e)
            r.append({"email": e, "password": pwd, "status": "正常",
                      "checked_at": mt.group(1) if mt else "", "country": mc.group(1) if mc else ""})

        if not r:
            r = from_inputs(driver)
        logger.info(f"  app.iosr.cn 抓到: {len(r)}")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  app.iosr.cn error: {ex}")
        return []


def crawl_shadowrocket_best(driver) -> list:
    """shadowrocket.best — 无限滚动，剪贴板方式"""
    url = "https://shadowrocket.best/"
    # requests 先试
    html = fetch_html(url)
    if html and "@" in html:
        r = strategy_data_clipboard(html)
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
            time.sleep(0.6)
            cards = driver.find_elements(By.CSS_SELECTOR,
                ".card,.id-card,.account-card,[class*='card'],[class*='item'],[class*='account']")
            if len(cards) == last_count:
                break
            last_count = len(cards)
        driver.execute_script("window.scrollTo(0,0)")
        time.sleep(1)

        r = click_all_copy_btns(driver)
        r = enrich_country_time(driver, r)
        if not r:
            r = js_full_scan(driver)
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
            r = js_full_scan(driver)
        logger.info(f"  ccbaohe [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  ccbaohe error: {ex}")
        return []


def crawl_tkbaohe(driver) -> list:
    """tkbaohe.com — 与ccbaohe完全相同结构"""
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
            r = js_full_scan(driver)
        logger.info(f"  tkbaohe [selenium] → {len(r)} 条")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  tkbaohe error: {ex}")
        return []


def crawl_id_btvda_top(driver) -> list:
    """
    id.btvda.top — 结构同 139.196：
    button#username_N[data-clipboard-text]=邮箱
    button#password_N[data-clipboard-text]=密码
    用 strategy_data_clipboard 按 id 精确配对，绝不错位
    """
    url = "https://id.btvda.top/"
    try:
        driver.get(url)
        time.sleep(6)
        close_popups(driver)
        scroll(driver, n=15)
        time.sleep(2)
        # 诊断：打印前6个 data-clipboard-text 按钮的结构
        try:
            diag = driver.execute_script("""
var btns = Array.from(document.querySelectorAll('[data-clipboard-text]')).slice(0,6);
return btns.map(function(b){
    return {id: b.id||'', cls: b.className||'', val: (b.getAttribute('data-clipboard-text')||'').slice(0,40)};
});
            """)
            logger.info(f"  btvda 按钮结构: {diag}")
        except Exception:
            pass
        # 用 strategy_data_clipboard 读 data-clipboard-text 属性（id精确配对）
        r = strategy_data_clipboard(driver.page_source)
        if not r:
            r = js_full_scan(driver)
        if not r:
            r = from_inputs(driver)
        logger.info(f"  id.btvda.top 抓到: {len(r)}")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  id.btvda.top error: {ex}")
        return []


def crawl_bocchi2b(driver) -> list:
    """id.bocchi2b.top — 剪贴板方式，有弹窗"""
    url = "https://id.bocchi2b.top/"
    html = fetch_html(url)
    if html and "@" in html:
        r = strategy_data_clipboard(html)
        if r:
            logger.info(f"  bocchi2b [requests] → {len(r)} 条")
            return dedup(r)

    try:
        driver.get(url)
        time.sleep(6)
        for _ in range(4):
            close_popups(driver)
            time.sleep(0.7)
        try:
            WebDriverWait(driver, 15).until(
                lambda d: "@" in d.page_source and len(d.page_source) > 5000
            )
        except Exception:
            pass
        scroll(driver, n=12)
        time.sleep(2)

        r = click_all_copy_btns(driver)
        r = enrich_country_time(driver, r)
        if not r:
            r = from_inputs(driver)
        if not r:
            r = js_full_scan(driver)
        logger.info(f"  bocchi2b 抓到: {len(r)}")
        return dedup(r)
    except Exception as ex:
        logger.error(f"  bocchi2b error: {ex}")
        return []


# ══════════════════════════════════════════
# 站点配置
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


def recheck_email_from_site(driver, site_name: str, target_email: str) -> str:
    """
    用备用方法从指定站点重新抓取某个邮箱的密码。
    备用方法和主方法不同，以便交叉验证。
    返回密码字符串，找不到返回 ""
    """
    logger.info(f"    [重抓] {site_name} 用备用方法查找 {target_email}")

    def find_pw(pairs):
        for p in pairs:
            if p.get("email", "").lower() == target_email:
                return p.get("password", "")
        return ""

    try:
        if site_name in ("ccbaohe.com/appleID",):
            # 主方法是 strategy_mailto_onclick，备用：Selenium + js_full_scan
            driver.get("https://ccbaohe.com/appleID/")
            time.sleep(8); close_popups(driver); scroll(driver, n=10); time.sleep(2)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

        elif site_name in ("tkbaohe.com",):
            driver.get("https://tkbaohe.com/Shadowrocket/")
            time.sleep(8); close_popups(driver); scroll(driver, n=10); time.sleep(2)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

        elif site_name in ("id.btvda.top",):
            # 备用：Selenium 加载后用 js_full_scan（主方法是 strategy_data_clipboard）
            driver.get("https://id.btvda.top/")
            time.sleep(6); close_popups(driver); scroll(driver, n=15); time.sleep(2)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

        elif site_name in ("id.bocchi2b.top",):
            # 备用：js_full_scan（主方法是 strategy_data_clipboard）
            driver.get("https://id.bocchi2b.top/")
            time.sleep(6)
            for _ in range(4): close_popups(driver); time.sleep(0.5)
            scroll(driver, n=12); time.sleep(2)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

        elif site_name in ("shadowrocket.best",):
            # 备用：js_full_scan（主方法是 strategy_data_clipboard）
            driver.get("https://shadowrocket.best/")
            time.sleep(6); close_popups(driver); scroll(driver, n=20); time.sleep(2)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

        elif site_name in ("139.196.183.52",):
            # 主是requests静态，备用：Selenium + click_all_copy_btns
            driver.get("http://139.196.183.52/share/DZhBvnglEU")
            time.sleep(5); close_popups(driver); scroll(driver, n=6); time.sleep(2)
            pairs = click_all_copy_btns(driver)
            pairs = enrich_country_time(driver, pairs)
            return find_pw(pairs)

        elif site_name in ("app.iosr.cn",):
            driver.get("https://app.iosr.cn/tools/apple-shared-id")
            time.sleep(7); close_popups(driver)
            try:
                driver.find_element(By.XPATH, "//button[contains(.,'刷新')]").click()
                time.sleep(4)
            except Exception:
                pass
            scroll(driver, n=10); time.sleep(2)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

        elif site_name in ("free.iosapp.icu",):
            html = fetch_html("https://free.iosapp.icu/")
            if html:
                pairs = parse_text(html)
                pw = find_pw(pairs)
                if pw:
                    return pw
            driver.get("https://free.iosapp.icu/")
            time.sleep(5); close_popups(driver); scroll(driver, n=6)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

        elif site_name in ("idfree.top",):
            driver.get("https://idfree.top/")
            time.sleep(8); close_popups(driver); scroll(driver, n=10); time.sleep(2)
            pairs = js_full_scan(driver) or from_inputs(driver)
            return find_pw(pairs)

    except Exception as ex:
        logger.debug(f"    [重抓] {site_name} 备用方法异常: {ex}")

    return ""


def resolve_conflicts(driver, conflicts: list, records: dict):
    """
    对所有密码冲突账号，用备用方法重抓两个来源，交叉验证决定保留哪个密码。
    conflicts: list of (email, src_a, pw_a, src_b, pw_b)
    直接修改 records
    """
    if not conflicts:
        return

    logger.info(f"\n{'='*50}")
    logger.info(f"  开始处理 {len(conflicts)} 个密码冲突...")

    for email, src_a, pw_a, src_b, pw_b in conflicts:
        logger.info(f"  冲突账号: {email}")
        logger.info(f"    来源A: {src_a} → {pw_a!r}")
        logger.info(f"    来源B: {src_b} → {pw_b!r}")

        # 用备用方法分别重抓
        recheck_a = recheck_email_from_site(driver, src_a, email)
        recheck_b = recheck_email_from_site(driver, src_b, email)

        logger.info(f"    重抓A({src_a}): {recheck_a!r}")
        logger.info(f"    重抓B({src_b}): {recheck_b!r}")

        final_pw = None

        if recheck_a and recheck_b:
            if recheck_a == recheck_b:
                # 两个备用结果一致 → 用它
                final_pw = recheck_a
                logger.info(f"    ✅ 两次重抓一致 → 密码={final_pw!r}")
            elif recheck_a == pw_a and recheck_b == pw_b:
                # 各自备用和原始一致，但两边还是不同 → 舍弃
                logger.info(f"    ❌ 两边各自一致但互不相同 → 舍弃该账号")
                records.pop(email, None)
                continue
            elif recheck_a == pw_a:
                # A备用和A原始一致，B备用和B原始不同 → 用A
                final_pw = pw_a
                logger.info(f"    ✅ A来源备用一致 → 密码={final_pw!r}")
            elif recheck_b == pw_b:
                # B备用和B原始一致，A备用和A原始不同 → 用B
                final_pw = pw_b
                logger.info(f"    ✅ B来源备用一致 → 密码={final_pw!r}")
            else:
                # 备用结果和原始都对不上 → 舍弃
                logger.info(f"    ❌ 重抓结果均不可靠 → 舍弃该账号")
                records.pop(email, None)
                continue
        elif recheck_a:
            # 只有A能重抓到
            if recheck_a == pw_a:
                final_pw = pw_a
                logger.info(f"    ✅ 仅A可重抓且一致 → 密码={final_pw!r}")
            else:
                logger.info(f"    ❌ A重抓结果和原始不同 → 舍弃该账号")
                records.pop(email, None)
                continue
        elif recheck_b:
            # 只有B能重抓到
            if recheck_b == pw_b:
                final_pw = pw_b
                logger.info(f"    ✅ 仅B可重抓且一致 → 密码={final_pw!r}")
            else:
                logger.info(f"    ❌ B重抓结果和原始不同 → 舍弃该账号")
                records.pop(email, None)
                continue
        else:
            # 两边都重抓不到 → 舍弃
            logger.info(f"    ❌ 两边都无法重抓 → 舍弃该账号")
            records.pop(email, None)
            continue

        if final_pw and email in records:
            records[email]["password"] = final_pw
            records[email]["source"] += f"+verified"

    logger.info(f"{'='*50}\n")

def crawl_all():
    records = {}
    source_stats = {}
    conflicts = []   # 密码冲突列表，格式: (email, src_a, pw_a, src_b, pw_b)

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
                # 过滤遮蔽邮箱：@前面的本地部分至少4个字符（d@4so8nn 这种只有1个字母的是遮蔽版）
                local_part = e.split("@")[0]
                if len(local_part) < 4:
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
                    existing = records[e]
                    existing_pw = existing.get("password", "")
                    if existing_pw == pw:
                        # 账号+密码完全相同：正常去重，只更新时间和国家
                        new_t = p.get("checked_at", "")
                        old_t = existing.get("checked_at", "")
                        if new_t and new_t > old_t:
                            existing["checked_at"] = new_t
                        if p.get("country") and not existing.get("country"):
                            existing["country"] = p["country"]
                    else:
                        # 账号相同但密码不同：记录冲突，等所有站点跑完后统一重抓验证
                        logger.warning(
                            f"  ⚠️  密码冲突 [{e}]: "
                            f"{existing.get('source')}={existing_pw!r} vs "
                            f"{site['name']}={pw!r}"
                        )
                        conflicts.append((e, existing.get("source"), existing_pw, site["name"], pw))
                        # 暂时补充国家/时间，密码待验证后决定
                        if p.get("country") and not existing.get("country"):
                            existing["country"] = p["country"]
                        new_t = p.get("checked_at", "")
                        old_t = existing.get("checked_at", "")
                        if new_t and new_t > old_t:
                            existing["checked_at"] = new_t

            source_stats[site["name"]] = nc
            total = len(records)
            logger.info(f"  → 新增 {nc} 条（共 {total} 条）"
                        f"  [抓到 {len(pairs)} 条，重复 {len(pairs)-nc} 条]")
            time.sleep(1)
        # 所有站点跑完后，处理密码冲突
        if conflicts:
            resolve_conflicts(driver, conflicts, records)
    finally:
        driver.quit()
        logger.info("浏览器已关闭")

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
