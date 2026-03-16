#!/usr/bin/env python3
"""
Apple ID 爬虫 - 整合版
抓取: idshare001.me / idfree.top / id.btvda.top / id.bocchi2b.top / ccbaohe.com / tkbaohe.com
"""
import re, json, os, logging, hashlib, time
from datetime import datetime, timezone, timedelta
import requests
from bs4 import BeautifulSoup

CST = timezone(timedelta(hours=8))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
for _h in logging.root.handlers:
    class _F(logging.Formatter):
        def formatTime(self, r, d=None):
            return datetime.fromtimestamp(r.created, tz=CST).strftime("%Y-%m-%d %H:%M:%S")
    _h.setFormatter(_F("%(asctime)s [%(levelname)s] %(message)s"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

VALID_DOMAINS = {
    "icloud.com","me.com","mac.com","gmail.com",
    "outlook.com","hotmail.com","live.com","msn.com",
    "qq.com","163.com","126.com","yahoo.com","yahoo.co.jp",
    "protonmail.com","proton.me","email.com",
}

def is_valid_email(e):
    if not e or "@" not in e: return False
    p = e.lower().split("@")
    return len(p) == 2 and len(p[0]) >= 4 and p[1] in VALID_DOMAINS

def uid(email): return hashlib.md5(email.encode()).hexdigest()[:12]

def now_cst(): return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")

def to_cst(ts):
    if not ts: return ""
    try:
        m = re.search(r"(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)", str(ts))
        if not m: return str(ts)
        clean = m.group(1).replace("T", " ")
        if len(clean) == 16: clean += ":00"
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S")
        return (dt + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    except: return str(ts)

def parse_api_list(raw, source, time_is_utc=True):
    results = []
    if not isinstance(raw, list): return results
    for item in raw:
        if not isinstance(item, dict): continue
        email = str(item.get("email") or item.get("username") or item.get("account") or "").strip().lower()
        pw = str(item.get("password") or item.get("pwd") or "").strip()
        if not is_valid_email(email) or not pw: continue
        rs = item.get("status", 1)
        if isinstance(rs, int) and rs == 0: continue
        if isinstance(rs, str) and any(k in rs for k in ["锁","异常","失效"]): continue
        country = str(item.get("country") or "")
        if not any(c in country for c in "美英日港台韩越澳新加法德俄巴"): country = "美国"
        ts = str(item.get("time") or item.get("checked_at") or "")
        results.append({
            "id": uid(email), "email": email, "password": pw,
            "status": "正常", "country": country,
            "checked_at": to_cst(ts) if time_is_utc else ts,
            "source": source, "updated_at": now_cst()
        })
    return results

# ── 各站爬取函数 ──────────────────────────────────────

def crawl_idshare001():
    results = []
    for i in [2, 1]:
        try:
            r = requests.get(f"https://idshare001.me/node/getid.php?getid={i}", headers=HEADERS, timeout=15)
            data = r.json()
            if isinstance(data, list):
                results.extend(parse_api_list(data, "idshare001.me", time_is_utc=False))
                logger.info(f"  idshare001 getid={i} → {len(data)} 条")
        except Exception as e: logger.warning(f"  idshare001 getid={i}: {e}")
    return results

def crawl_idfree():
    s = requests.Session()
    s.headers.update({**HEADERS, "Referer": "https://idfree.top/"})
    token = None
    try:
        r = s.get("https://idfree.top/api/session_verify.php", timeout=10)
        token = r.json().get("token")
        logger.info(f"  idfree token={token}")
    except Exception as e:
        logger.warning(f"  idfree token: {e}")
        return []
    if not token: return []
    for url in [
        f"https://idfree.top/api/accounts.php?token={token}",
        "https://idfree.top/api/accounts.php",
    ]:
        try:
            s.cookies.set("token", token)
            s.cookies.set("session_token", token)
            s.headers.update({"X-Token": token, "X-Auth-Token": token})
            r = s.get(url, timeout=15)
            logger.info(f"  idfree status={r.status_code} body={r.text[:150]}")
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    logger.info(f"  idfree 抓到 {len(data)} 条")
                    return parse_api_list(data, "idfree.top", time_is_utc=False)
        except Exception as e: logger.warning(f"  idfree {url}: {e}")
    return []

def crawl_btvda():
    try:
        h = {**HEADERS, "Referer": "https://id.btvda.top/", "Origin": "https://id.btvda.top"}
        r = requests.get("https://appleapi.omofunz.com/api/data", headers=h, timeout=15)
        data = r.json()
        if isinstance(data, list):
            logger.info(f"  btvda 抓到 {len(data)} 条")
            return parse_api_list(data, "id.btvda.top", time_is_utc=True)
    except Exception as e: logger.warning(f"  btvda: {e}")
    return []

def crawl_bocchi2b():
    password = "qFyxno"
    s = requests.Session()
    s.headers.update({**HEADERS, "Referer": "https://id.bocchi2b.top/", "Origin": "https://id.bocchi2b.top"})
    try:
        r = s.get("https://id.bocchi2b.top/", timeout=10)
        m = re.search(r'password[=:][\x27"](\w{4,20})[\x27"]', r.text)
        if not m: m = re.search(r'[?&]password=(\w{4,20})', r.text)
        if m: password = m.group(1); logger.info(f"  bocchi2b password={password}")
    except Exception as e: logger.warning(f"  bocchi2b home: {e}")
    try:
        r = s.get(f"https://id.bocchi2b.top/api/list?password={password}", timeout=15)
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else (
                data.get("id") or data.get("data") or data.get("list") or []
            ) if isinstance(data, dict) else []
            if items:
                logger.info(f"  bocchi2b 抓到 {len(items)} 条")
                return parse_api_list(items, "id.bocchi2b.top", time_is_utc=False)
    except Exception as e: logger.warning(f"  bocchi2b: {e}")
    return []

def decode_cfemail(encoded):
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except: return ""

def crawl_mailto_site(url, source):
    """通用：解析 Cloudflare 保护邮箱 + onclick copy密码"""
    try:
        r = requests.get(url, headers={**HEADERS, "Referer": url}, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        results = []
        for tag in soup.select("[data-cfemail]"):
            enc = tag.get("data-cfemail", "")
            email = decode_cfemail(enc).lower().strip()
            if not is_valid_email(email): continue
            # 找同行或父级的密码按钮
            parent = tag.find_parent()
            for _ in range(5):
                if not parent: break
                btns = parent.find_all("button", onclick=True)
                for btn in btns:
                    m = re.search(r"copy\(['\"]([^'\"]+)['\"]\)", btn.get("onclick", ""))
                    if m:
                        pw = m.group(1).strip()
                        if pw and "@" not in pw and len(pw) >= 4:
                            results.append({
                                "id": uid(email), "email": email, "password": pw,
                                "status": "正常", "country": "美国",
                                "checked_at": "", "source": source, "updated_at": now_cst()
                            })
                            break
                parent = parent.find_parent()
        logger.info(f"  {source} 抓到 {len(results)} 条")
        return results
    except Exception as e:
        logger.warning(f"  {source}: {e}")
        return []

def crawl_ccbaohe():
    return crawl_mailto_site("https://ccbaohe.com/appleID", "ccbaohe.com/appleID")

def crawl_tkbaohe():
    return crawl_mailto_site("https://tkbaohe.com/Shadowrocket", "tkbaohe.com")

# ── 合并 & 保存 ──────────────────────────────────────

SITE_ORDER = [
    "idshare001.me", "idfree.top", "ccbaohe.com/appleID",
    "tkbaohe.com", "id.btvda.top", "id.bocchi2b.top"
]

def merge_and_save(new_accounts, output_file):
    existing = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, encoding="utf-8") as f:
                for a in json.load(f).get("accounts", []):
                    existing[a["email"]] = a
        except: pass

    for a in new_accounts:
        e = a["email"]
        if e not in existing:
            existing[e] = a
        else:
            # 补充缺失字段
            if a.get("country") and not existing[e].get("country"):
                existing[e]["country"] = a["country"]
            if a.get("checked_at") and a["checked_at"] > existing[e].get("checked_at", ""):
                existing[e]["checked_at"] = a["checked_at"]

    accounts = sorted(
        existing.values(),
        key=lambda x: SITE_ORDER.index(x.get("source", "")) if x.get("source", "") in SITE_ORDER else 99
    )

    ss = {}
    for a in accounts:
        src = a.get("source", "")
        ss[src] = ss.get(src, 0) + 1

    result = {
        "total": len(accounts),
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M"),
        "source_stats": ss,
        "accounts": accounts
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return len(accounts)

# ── 主流程 ──────────────────────────────────────────

def main():
    logger.info("🚀 爬虫启动")
    start = time.time()

    crawlers = [
        ("idshare001", crawl_idshare001),
        ("idfree",     crawl_idfree),
        ("btvda",      crawl_btvda),
        ("bocchi2b",   crawl_bocchi2b),
        ("ccbaohe",    crawl_ccbaohe),
        ("tkbaohe",    crawl_tkbaohe),
    ]

    all_accounts = []
    for name, fn in crawlers:
        try:
            r = fn()
            all_accounts.extend(r)
            logger.info(f"  ▶ {name}: {len(r)} 条")
        except Exception as e:
            logger.error(f"  ▶ {name} 出错: {e}")

    output = os.environ.get("OUTPUT_FILE", "apple_ids.json")
    total = merge_and_save(all_accounts, output)
    logger.info(f"✅ 完成！共 {total} 条，耗时 {time.time()-start:.1f}秒 → {output}")

if __name__ == "__main__":
    main()
