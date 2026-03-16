#!/usr/bin/env python3
"""
快速爬虫 - 纯 requests，无浏览器
抓取有直接 API 的站点：
1. idshare001.me   → /node/getid.php?getid=1&2
2. idfree.top      → /api/accounts.php
3. id.btvda.top    → https://appleapi.omofunz.com/api/data
4. id.bocchi2b.top → /api/list
运行时间：< 30秒
"""

import re, json, os, logging, hashlib, time
from datetime import datetime, timezone, timedelta

import requests

CST = timezone(timedelta(hours=8))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

for _h in logging.root.handlers:
    class _F(logging.Formatter):
        def formatTime(self, r, d=None):
            return datetime.fromtimestamp(r.created, tz=CST).strftime('%Y-%m-%d %H:%M:%S')
    _h.setFormatter(_F('%(asctime)s [%(levelname)s] %(message)s'))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

VALID_DOMAINS = {
    "icloud.com","me.com","mac.com","gmail.com","outlook.com","hotmail.com",
    "live.com","msn.com","qq.com","163.com","126.com","yahoo.com",
    "protonmail.com","proton.me","email.com",
}

def is_valid_email(e):
    if not e or "@" not in e: return False
    parts = e.lower().split("@")
    if len(parts) != 2: return False
    return len(parts[0]) >= 4 and parts[1] in VALID_DOMAINS

def uid(email):
    return hashlib.md5(email.encode()).hexdigest()[:12]

def to_cst(ts):
    """UTC时间字符串转北京时间"""
    if not ts: return ""
    try:
        m = re.search(r"(20\d{2}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)", str(ts))
        if not m: return str(ts)
        clean = m.group(1).replace("T", " ")
        if len(clean) == 16: clean += ":00"
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S")
        return (dt + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)

def parse_api_list(raw, source, time_is_utc=True):
    """解析 API 返回的账号列表"""
    results = []
    if not isinstance(raw, list): return results
    for item in raw:
        if not isinstance(item, dict): continue
        email = str(item.get("email") or item.get("username") or item.get("account") or "").strip().lower()
        pw = str(item.get("password") or item.get("pwd") or "").strip()
        if not is_valid_email(email) or not pw: continue
        raw_status = item.get("status", 1)
        if isinstance(raw_status, int) and raw_status == 0: continue
        if isinstance(raw_status, str) and any(k in raw_status for k in ["锁","异常","失效"]): continue
        country = str(item.get("country") or "")
        ts = str(item.get("time") or item.get("checked_at") or "")
        results.append({
            "id": uid(email),
            "email": email,
            "password": pw,
            "status": "正常",
            "country": country if any(c in country for c in "美英日港台韩越澳新加法德俄巴") else "美国",
            "checked_at": to_cst(ts) if time_is_utc else ts,
            "source": source,
            "updated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return results

def fetch(url, timeout=15):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.warning(f"  请求失败 {url}: {e}")
    return None

def crawl_idshare001():
    results = []
    for i in [2, 1]:
        data = fetch(f"https://idshare001.me/node/getid.php?getid={i}")
        if isinstance(data, list):
            results.extend(parse_api_list(data, "idshare001.me", time_is_utc=False))
            logger.info(f"  idshare001 getid={i} → {len(data)} 条")
    return results

def crawl_idfree():
    # 先获取 token
    token = None
    try:
        r = requests.get("https://idfree.top/api/session_verify.php", headers=HEADERS, timeout=10)
        token = r.json().get("token")
    except Exception:
        pass

    headers = {**HEADERS, **({"Authorization": f"Bearer {token}"} if token else {})}
    try:
        r = requests.get("https://idfree.top/api/accounts.php", headers=headers, timeout=15)
        data = r.json()
        if isinstance(data, list):
            logger.info(f"  idfree 抓到 {len(data)} 条")
            return parse_api_list(data, "idfree.top", time_is_utc=False)
    except Exception as e:
        logger.warning(f"  idfree 失败: {e}")
    return []

def crawl_btvda():
    data = fetch("https://appleapi.omofunz.com/api/data")
    if isinstance(data, list):
        logger.info(f"  btvda 抓到 {len(data)} 条")
        return parse_api_list(data, "id.btvda.top", time_is_utc=True)
    return []

def crawl_bocchi2b():
    # 先尝试获取最新密码参数
    password = "qFyxno"  # 上次嗅探到的密码，可能会变
    try:
        # 从配置或页面获取最新密码
        r = requests.get("https://id.bocchi2b.top/", headers=HEADERS, timeout=10)
        m = re.search(r'password[=:][\'"]([\w]+)[\'"]', r.text)
        if m:
            password = m.group(1)
    except Exception:
        pass

    data = fetch(f"https://id.bocchi2b.top/api/list?password={password}")
    if isinstance(data, dict):
        items = data.get("id") or data.get("data") or data.get("list") or []
        if isinstance(items, list):
            logger.info(f"  bocchi2b 抓到 {len(items)} 条")
            return parse_api_list(items, "id.bocchi2b.top", time_is_utc=False)
    elif isinstance(data, list):
        logger.info(f"  bocchi2b 抓到 {len(data)} 条")
        return parse_api_list(data, "id.bocchi2b.top", time_is_utc=False)
    return []

def merge_with_existing(new_accounts, output_file):
    """把新抓到的账号合并到现有 JSON，只更新快速站点的数据"""
    existing = {}
    fast_sources = {"idshare001.me", "idfree.top", "id.btvda.top", "id.bocchi2b.top"}

    if os.path.exists(output_file):
        try:
            with open(output_file) as f:
                old = json.load(f)
            for a in old.get("accounts", []):
                # 保留慢速站点（ccbaohe/tkbaohe）的数据不变
                existing[a["email"]] = a
        except Exception:
            pass

    # 用新数据覆盖快速站点的账号
    for a in new_accounts:
        existing[a["email"]] = a

    accounts = sorted(existing.values(), key=lambda x: (
        ["idshare001.me","idfree.top","ccbaohe.com/appleID","tkbaohe.com","id.btvda.top","id.bocchi2b.top"]
        .index(x.get("source","")) if x.get("source","") in
        ["idshare001.me","idfree.top","ccbaohe.com/appleID","tkbaohe.com","id.btvda.top","id.bocchi2b.top"]
        else 99
    ))

    source_stats = {}
    for a in accounts:
        s = a.get("source","")
        source_stats[s] = source_stats.get(s, 0) + 1

    result = {
        "total": len(accounts),
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M"),
        "source_stats": source_stats,
        "accounts": accounts,
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return len(accounts)

def main():
    logger.info("🚀 快速爬虫启动")
    start = time.time()

    all_accounts = []
    for name, fn in [
        ("idshare001", crawl_idshare001),
        ("idfree",     crawl_idfree),
        ("btvda",      crawl_btvda),
        ("bocchi2b",   crawl_bocchi2b),
    ]:
        try:
            r = fn()
            all_accounts.extend(r)
            logger.info(f"  ▶ {name}: {len(r)} 条")
        except Exception as e:
            logger.error(f"  ▶ {name} 出错: {e}")

    output = os.environ.get("OUTPUT_FILE", "apple_ids.json")
    total = merge_with_existing(all_accounts, output)

    elapsed = time.time() - start
    logger.info(f"✅ 完成！共 {total} 条账号，耗时 {elapsed:.1f}秒 → {output}")

if __name__ == "__main__":
    main()
