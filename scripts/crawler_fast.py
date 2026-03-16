#!/usr/bin/env python3
import re, json, os, logging, hashlib, time
from datetime import datetime, timezone, timedelta
import requests

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
VALID_DOMAINS = {"icloud.com","me.com","mac.com","gmail.com","outlook.com","hotmail.com","live.com","msn.com","qq.com","163.com","126.com","yahoo.com","protonmail.com","proton.me","email.com"}

def is_valid_email(e):
    if not e or "@" not in e: return False
    p = e.lower().split("@")
    return len(p)==2 and len(p[0])>=4 and p[1] in VALID_DOMAINS

def uid(email): return hashlib.md5(email.encode()).hexdigest()[:12]

def to_cst(ts):
    if not ts: return ""
    try:
        m = re.search(r"(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)", str(ts))
        if not m: return str(ts)
        clean = m.group(1).replace("T"," ")
        if len(clean)==16: clean+=":00"
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S")
        return (dt+timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
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
        if isinstance(rs, int) and rs==0: continue
        if isinstance(rs, str) and any(k in rs for k in ["锁","异常","失效"]): continue
        country = str(item.get("country") or "")
        cc = country if any(c in country for c in "美英日港台韩越澳新加法德俄巴") else "美国"
        ts = str(item.get("time") or item.get("checked_at") or "")
        results.append({"id":uid(email),"email":email,"password":pw,"status":"正常",
            "country":cc,"checked_at":to_cst(ts) if time_is_utc else ts,
            "source":source,"updated_at":datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")})
    return results

def crawl_idshare001():
    results = []
    for i in [2,1]:
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
    s.headers.update({**HEADERS, "Referer":"https://idfree.top/"})
    token = None
    try:
        r = s.get("https://idfree.top/api/session_verify.php", timeout=10)
        logger.info(f"  idfree verify status={r.status_code} body={r.text[:100]}")
        token = r.json().get("token")
    except Exception as e: logger.warning(f"  idfree token: {e}")
    if token:
        # token 放 cookie，不放 header
        s.cookies.set("token", token, domain="idfree.top")
    try:
        r = s.get("https://idfree.top/api/accounts.php", timeout=15)
        logger.info(f"  idfree accounts status={r.status_code} body={r.text[:200]}")
        data = r.json()
        if isinstance(data, list):
            logger.info(f"  idfree 抓到 {len(data)} 条")
            return parse_api_list(data, "idfree.top", time_is_utc=False)
    except Exception as e: logger.warning(f"  idfree: {e}")
    return []

def crawl_btvda():
    try:
        h = {**HEADERS, "Referer":"https://id.btvda.top/", "Origin":"https://id.btvda.top"}
        r = requests.get("https://appleapi.omofunz.com/api/data", headers=h, timeout=15)
        logger.info(f"  btvda status={r.status_code} body={r.text[:100]}")
        data = r.json()
        if isinstance(data, list):
            logger.info(f"  btvda 抓到 {len(data)} 条")
            return parse_api_list(data, "id.btvda.top", time_is_utc=True)
    except Exception as e: logger.warning(f"  btvda: {e}")
    return []

def crawl_bocchi2b():
    password = "qFyxno"
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        # 先访问主页获取 session cookie
        r = s.get("https://id.bocchi2b.top/", timeout=10)
        logger.info(f"  bocchi2b home status={r.status_code} cookies={dict(s.cookies)}")
        m = re.search(r'password[=:][\x27"](\w{4,20})[\x27"]', r.text)
        if not m: m = re.search(r'[?&]password=(\w{4,20})', r.text)
        if m: password = m.group(1); logger.info(f"  bocchi2b password={password}")
    except Exception as e: logger.warning(f"  bocchi2b home: {e}")
    try:
        s.headers.update({"Referer": "https://id.bocchi2b.top/"})
        r = s.get(f"https://id.bocchi2b.top/api/list?password={password}", timeout=15)
        logger.info(f"  bocchi2b status={r.status_code} body={r.text[:200]}")
        data = r.json()
        items = data if isinstance(data,list) else (data.get("id") or data.get("data") or data.get("list") or []) if isinstance(data,dict) else []
        if items:
            logger.info(f"  bocchi2b 抓到 {len(items)} 条")
            return parse_api_list(items, "id.bocchi2b.top", time_is_utc=False)
    except Exception as e: logger.warning(f"  bocchi2b: {e}")
    return []

def merge_with_existing(new_accounts, output_file):
    existing = {}
    if os.path.exists(output_file):
        try:
            with open(output_file) as f:
                for a in json.load(f).get("accounts",[]): existing[a["email"]] = a
        except: pass
    for a in new_accounts: existing[a["email"]] = a
    SITE_ORDER = ["idshare001.me","idfree.top","ccbaohe.com/appleID","tkbaohe.com","id.btvda.top","id.bocchi2b.top"]
    accounts = sorted(existing.values(), key=lambda x: SITE_ORDER.index(x.get("source","")) if x.get("source","") in SITE_ORDER else 99)
    ss = {}
    for a in accounts: ss[a.get("source","")] = ss.get(a.get("source",""),0)+1
    result = {"total":len(accounts),"generated_at":datetime.now(CST).strftime("%Y-%m-%d %H:%M"),"source_stats":ss,"accounts":accounts}
    with open(output_file,"w",encoding="utf-8") as f: json.dump(result,f,ensure_ascii=False,indent=2)
    return len(accounts)

def main():
    logger.info("🚀 快速爬虫启动")
    start = time.time()
    all_accounts = []
    for name, fn in [("idshare001",crawl_idshare001),("idfree",crawl_idfree),("btvda",crawl_btvda),("bocchi2b",crawl_bocchi2b)]:
        try:
            r = fn(); all_accounts.extend(r); logger.info(f"  ▶ {name}: {len(r)} 条")
        except Exception as e: logger.error(f"  ▶ {name} 出错: {e}")
    output = os.environ.get("OUTPUT_FILE","apple_ids.json")
    total = merge_with_existing(all_accounts, output)
    logger.info(f"✅ 完成！共 {total} 条，耗时 {time.time()-start:.1f}秒 → {output}")

if __name__ == "__main__":
    main()
