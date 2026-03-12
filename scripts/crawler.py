#!/usr/bin/env python3
"""
Apple ID 共享账号爬虫
使用 Selenium 模拟真实浏览器，绕过反爬虫保护
"""

import re
import json
import time
import hashlib
import logging
import os
from datetime import datetime, timezone

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  站点配置
# ══════════════════════════════════════════════════════════════
SITES = [
    {"name": "free.iosapp.icu",  "url": "https://free.iosapp.icu/",                         "wait": 3},
    {"name": "idfree.top",       "url": "https://idfree.top/",                               "wait": 3},
    {"name": "id.btvda.top",     "url": "https://id.btvda.top/",                             "wait": 3},
    {"name": "idshare001.me",    "url": "https://idshare001.me/goso.html",                   "wait": 3},
    {"name": "ccbaohe.com",      "url": "https://ccbaohe.com/appleID/",                      "wait": 4},
    {"name": "ccbaohe.com-2",    "url": "https://ccbaohe.com/appleID2/",                     "wait": 4},
    {"name": "app.iosr.cn",      "url": "https://app.iosr.cn/tools/apple-shared-id",         "wait": 5},
    {"name": "clashid.com.cn",   "url": "https://clashid.com.cn/shadowrocket-apple-id",      "wait": 3},
    {"name": "ios.aneeo.com",    "url": "https://ios.aneeo.com/",                            "wait": 3},
    {"name": "appledi.com",      "url": "https://appledi.com/",                              "wait": 3},
    {"name": "nodeba.com",       "url": "http://nodeba.com/",                                "wait": 3},
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
    r"\s*[/\|｜,，:：\s]+\s*"
    r"([A-Za-z0-9!@#$%^&*()\-_=+]{6,32})",
    re.IGNORECASE,
)

# ══════════════════════════════════════════════════════════════
#  浏览器初始化
# ══════════════════════════════════════════════════════════════
def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,800")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=opts)
    # 隐藏 webdriver 特征
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    return driver

# ══════════════════════════════════════════════════════════════
#  文本解析
# ══════════════════════════════════════════════════════════════
def extract_from_text(text: str) -> list:
    results = []
    seen = set()

    # 策略1：同行 email/password
    for m in INLINE_PAIR_RE.finditer(text):
        email, pwd = m.group(1).lower(), m.group(2)
        if (email, pwd) not in seen:
            seen.add((email, pwd))
            results.append({"email": email, "password": pwd})

    # 策略2：上下文关联
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
                k = (email.lower(), pwd)
                if k not in seen:
                    seen.add(k)
                    results.append({"email": email.lower(), "password": pwd})
    return results

# ══════════════════════════════════════════════════════════════
#  单站点抓取
# ══════════════════════════════════════════════════════════════
def crawl_site(driver: webdriver.Chrome, site: dict) -> list:
    try:
        driver.get(site["url"])
        # 等待页面加载
        time.sleep(site.get("wait", 3))

        # 尝试等待 body 出现内容
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except Exception:
            pass

        # 滚动页面触发懒加载
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        # 获取渲染后的完整 HTML
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "head"]):
            tag.decompose()
        text = soup.get_text(separator="\n")

        pairs = extract_from_text(text)
        logger.info(f"  → 解析到 {len(pairs)} 条原始数据")
        return pairs

    except Exception as e:
        logger.error(f"  抓取失败: {e}")
        return []

# ══════════════════════════════════════════════════════════════
#  主逻辑
# ══════════════════════════════════════════════════════════════
def uid(email: str) -> str:
    return hashlib.md5(email.lower().encode()).hexdigest()[:12]

def crawl_all() -> dict:
    seen: dict         = {}
    source_stats: dict = {}

    logger.info("启动浏览器...")
    driver = make_driver()

    try:
        for site in SITES:
            logger.info(f"▶ 抓取: {site['name']}  {site['url']}")
            pairs = crawl_site(driver, site)

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
