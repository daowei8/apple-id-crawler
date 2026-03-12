#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫 - 多站点抓取、去重、结构化输出
"""

import re
import json
import time
import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  站点配置（共 11 个）
# ══════════════════════════════════════════════════════════════
SITES = [
    {
        "name": "free.iosapp.icu",
        "url":  "https://free.iosapp.icu/",
        "strategy": "generic",
    },
    {
        "name": "idfree.top",
        "url":  "https://idfree.top/",
        "strategy": "generic",
    },
    {
        "name": "id.btvda.top",
        "url":  "https://id.btvda.top/",
        "strategy": "generic",
    },
    {
        "name": "idshare001.me",
        "url":  "https://idshare001.me/goso.html",
        "strategy": "generic",
    },
    {
        "name": "ccbaohe.com",
        "url":  "https://ccbaohe.com/appleID/",
        "strategy": "generic",
    },
    {
        "name": "ccbaohe.com-2",
        "url":  "https://ccbaohe.com/appleID2/",
        "strategy": "generic",
    },
    {
        "name": "app.iosr.cn",
        "url":  "https://app.iosr.cn/tools/apple-shared-id",
        "strategy": "api_json",
    },
    {
        "name": "clashid.com.cn",
        "url":  "https://clashid.com.cn/shadowrocket-apple-id",
        "strategy": "generic",
    },
    {
        "name": "ios.aneeo.com",
        "url":  "https://ios.aneeo.com/",
        "strategy": "generic",
    },
    {
        "name": "appledi.com",
        "url":  "https://appledi.com/",
        "strategy": "generic",
    },
    {
        "name": "nodeba.com",
        "url":  "http://nodeba.com/",
        "strategy": "generic",
    },
]

# ══════════════════════════════════════════════════════════════
#  正则模式
# ══════════════════════════════════════════════════════════════
APPLE_ID_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@(?:icloud|me|mac|apple|gmail|qq|163|126|hotmail|outlook|yahoo|proton|pm)\.[a-z]{2,}\b",
    re.IGNORECASE,
)
PASSWORD_CONTEXT_RE = re.compile(
    r"(?:密[码碼]|pass(?:word)?|pwd)\s*[：:=\s]\s*([A-Za-z0-9!@#$%^&*()\-_=+\[\]{};:,./<>?]{6,32})",
    re.IGNORECASE,
)
INLINE_PAIR_RE = re.compile(
    r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,})"
    r"\s*[/\|｜\\s,，:：]+\s*"
    r"([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})",
    re.IGNORECASE,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ══════════════════════════════════════════════════════════════
#  工具函数
# ══════════════════════════════════════════════════════════════
def uid(email: str) -> str:
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]

def fetch(url: str, timeout: int = 15, retries: int = 3) -> Optional[requests.Response]:
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning(f"  [{i+1}/{retries}] 失败 {url}: {e}")
            time.sleep(2 ** i)
    return None

# ══════════════════════════════════════════════════════════════
#  解析策略
# ══════════════════════════════════════════════════════════════
def extract_from_text(text: str) -> list:
    results = []
    seen_pairs = set()

    for m in INLINE_PAIR_RE.finditer(text):
        email, pwd = m.group(1).lower(), m.group(2)
        key = (email, pwd)
        if key not in seen_pairs:
            seen_pairs.add(key)
            results.append({"email": email, "password": pwd})

    lines = text.splitlines()
    for i, line in enumerate(lines):
        emails = APPLE_ID_RE.findall(line)
        if not emails:
            continue
        context = "\n".join(lines[max(0, i - 2): i + 5])
        m = PASSWORD_CONTEXT_RE.search(context)
        if m:
            pwd = m.group(1).strip()
            for email in emails:
                key = (email.lower(), pwd)
                if key not in seen_pairs:
                    seen_pairs.add(key)
                    results.append({"email": email.lower(), "password": pwd})
    return results

def strategy_generic(site: dict) -> list:
    resp = fetch(site["url"])
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script", "style", "noscript", "head"]):
        tag.decompose()
    return extract_from_text(soup.get_text(separator="\n"))

def strategy_api_json(site: dict) -> list:
    resp = fetch(site["url"])
    if not resp:
        return []
    try:
        data = resp.json()
        results = []
        items = data if isinstance(data, list) else \
            data.get("data", data.get("list", data.get("items", data.get("accounts", []))))
        for item in (items if isinstance(items, list) else []):
            if not isinstance(item, dict):
                continue
            email = (item.get("apple_id") or item.get("email") or
                     item.get("account") or item.get("username") or "").lower()
            pwd = str(item.get("password") or item.get("passwd") or item.get("pwd") or "")
            if email and pwd and "@" in email:
                results.append({"email": email, "password": pwd})
        if results:
            return results
    except Exception:
        pass
    return strategy_generic(site)

STRATEGIES = {
    "generic":  strategy_generic,
    "api_json": strategy_api_json,
}

# ══════════════════════════════════════════════════════════════
#  主逻辑
# ══════════════════════════════════════════════════════════════
def crawl_all() -> dict:
    seen: dict         = {}
    source_stats: dict = {}

    for site in SITES:
        logger.info(f"▶ 抓取: {site['name']}  {site['url']}")
        try:
            pairs = STRATEGIES.get(site["strategy"], strategy_generic)(site)
        except Exception as e:
            logger.error(f"  站点异常 {site['name']}: {e}")
            pairs = []

        new_count = 0
        now = datetime.now(timezone.utc).isoformat()
        for p in pairs:
            email = p.get("email", "").strip().lower()
            pwd   = p.get("password", "").strip()
            if not email or not pwd or "@" not in email:
                continue
            if email not in seen:
                seen[email] = {
                    "id":         uid(email),
                    "email":      email,
                    "password":   pwd,
                    "source":     site["name"],
                    "updated_at": now,
                }
                new_count += 1

        source_stats[site["name"]] = new_count
        logger.info(f"  → 新增 {new_count} 条（去重后共 {len(seen)} 条）")
        time.sleep(1.5)

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
