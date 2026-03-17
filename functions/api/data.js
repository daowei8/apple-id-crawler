export async function onRequest(context) {
  const GITHUB_RAW = 'https://raw.githubusercontent.com/daowei8/apple-id-crawler/main/apple_ids.json';
  
  const cacheKey = new Request(GITHUB_RAW);
  const cache = caches.default;
  
  // 先查缓存
  let cached = await cache.match(cacheKey);
  if (cached) {
    return new Response(cached.body, {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
        'X-Cache': 'HIT',
      },
    });
  }

  // 缓存没有，去 GitHub 拉
  try {
    const resp = await fetch(GITHUB_RAW, { cf: { cacheEverything: false } });
    if (!resp.ok) {
      return new Response(JSON.stringify({ error: 'GitHub fetch failed: ' + resp.status }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    const text = await resp.text();
    
    // 存入缓存 30 秒
    const toCache = new Response(text, {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'public, max-age=30',
      },
    });
    context.waitUntil(cache.put(cacheKey, toCache.clone()));
    
    return new Response(text, {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
        'X-Cache': 'MISS',
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
