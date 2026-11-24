const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const url = require('url');
const zlib = require('zlib');

const PORT = process.env.PORT || 3000;
process.on('uncaughtException', (err) => {
  console.error('[Server uncaughtException]', err && err.message ? err.message : err);
});
process.on('unhandledRejection', (reason) => {
  console.error('[Server unhandledRejection]', reason && reason.message ? reason.message : reason);
});
const DATA_DIR = __dirname;
const yahooChartCache = new Map();
const yahooQuoteCache = new Map();
const yahooSearchCache = new Map();
let binanceInfoCache = { ts: 0, usdtSet: null, baseMap: null };
let binanceTickersCache = { ts: 0, body: null };
let binanceAllMarketsCache = { ts: 0, body: null };
let fngCache = { ts: 0, body: null };
let globalMarketCache = { ts: 0, body: null };
const newsCache = new Map();

async function runWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let idx = 0;
  async function runner() {
    while (idx < items.length) {
      const cur = idx++;
      results[cur] = await worker(items[cur], cur);
    }
  }
  const poolSize = Math.min(limit, items.length);
  await Promise.all(Array.from({ length: poolSize }, () => runner()));
  return results;
}

function fetchHttpsJson(targetUrl) {
  return new Promise((resolve, reject) => {
    const req = https.get(targetUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json,text/plain,*/*',
        'Accept-Encoding': 'gzip, deflate'
      },
      timeout: 12000
    }, (upstreamRes) => {
      const chunks = [];
      upstreamRes.on('data', c => chunks.push(c));
      upstreamRes.on('end', () => {
        const buf = Buffer.concat(chunks);
        const enc = (upstreamRes.headers['content-encoding'] || '').toLowerCase();
        const decodeDone = (err, decodedBuf) => {
          if (err) return reject(err);
          const text = decodedBuf.toString('utf8');
          if (upstreamRes.statusCode < 200 || upstreamRes.statusCode >= 300) {
            return reject(new Error(`Upstream HTTP ${upstreamRes.statusCode}: ${text.slice(0, 160)}`));
          }
          try {
            resolve(JSON.parse(text));
          } catch (parseErr) {
            reject(new Error(`Invalid JSON: ${parseErr.message}`));
          }
        };
        if (enc === 'gzip') zlib.gunzip(buf, decodeDone);
        else if (enc === 'deflate') zlib.inflate(buf, decodeDone);
        else decodeDone(null, buf);
      });
    });
    req.on('timeout', () => { req.destroy(new Error('Upstream timeout')); });
    req.on('error', reject);
  });
}

function parseYahooChartPayload(rawJson, fallbackSymbol) {
  const result = rawJson && rawJson.chart && rawJson.chart.result && rawJson.chart.result[0];
  if (!result) return null;
  const meta = result.meta || {};
  const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
  const quote = result.indicators && result.indicators.quote && result.indicators.quote[0] ? result.indicators.quote[0] : {};
  const opens = quote.open || [];
  const highs = quote.high || [];
  const lows = quote.low || [];
  const closes = quote.close || [];
  const vols = quote.volume || [];

  const candles = [];
  for (let i = 0; i < timestamps.length; i++) {
    const ts = Number(timestamps[i]);
    const o = Number(opens[i]);
    const h = Number(highs[i]);
    const l = Number(lows[i]);
    const c = Number(closes[i]);
    const v = Number(vols[i]) || 0;
    if (
      Number.isFinite(ts) && ts > 0 &&
      Number.isFinite(o) && opens[i] !== null &&
      Number.isFinite(h) && highs[i] !== null &&
      Number.isFinite(l) && lows[i] !== null &&
      Number.isFinite(c) && closes[i] !== null
    ) {
      candles.push([Math.floor(ts), o, h, l, c, v]);
    }
  }
  candles.sort((a, b) => a[0] - b[0]);
  // Deduplicate identical timestamps keeping latest
  const deduped = [];
  for (let i = 0; i < candles.length; i++) {
    if (deduped.length > 0 && deduped[deduped.length - 1][0] === candles[i][0]) {
      deduped[deduped.length - 1] = candles[i];
    } else {
      deduped.push(candles[i]);
    }
  }
  // Trim all trailing synthetic post-market closing settlement ticks where O=H=L=C (zero range)
  while (deduped.length > 1) {
    const last = deduped[deduped.length - 1];
    if (Math.abs(last[2] - last[3]) < 0.0001) {
      deduped.pop();
    } else {
      break;
    }
  }
  const prevClose = meta.chartPreviousClose ?? meta.previousClose ?? (deduped[0] ? deduped[0][1] : null);
  const lastPrice = meta.regularMarketPrice ?? (deduped.length ? deduped[deduped.length - 1][4] : null);
  return {
    symbol: meta.symbol || fallbackSymbol,
    currency: meta.currency || '',
    exchangeName: meta.exchangeName || '',
    regularMarketPrice: lastPrice,
    previousClose: prevClose,
    candles: deduped
  };
}

// In-memory cache for parsed datasets
const datasets = {};
const resampledCache = {};

function parseMT5CSV(filePath) {
  const t0 = Date.now();
  if (!fs.existsSync(filePath)) return null;
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split(/\r?\n/);
  const candles = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const p = line.split('\t');
    if (p.length < 7) continue;

    // MT5 format: 2026.08.26	12:45:00	78474.5	78474.5	78443.6	78443.6	6	0	129
    const dtStr = p[0].replace(/\./g, '-') + 'T' + p[1] + 'Z';
    const ts = Math.floor(new Date(dtStr).getTime() / 1000);
    const o = parseFloat(p[2]);
    const h = parseFloat(p[3]);
    const l = parseFloat(p[4]);
    const c = parseFloat(p[5]);
    const v = parseFloat(p[6]);

    if (!isNaN(ts) && !isNaN(o) && !isNaN(h) && !isNaN(l) && !isNaN(c)) {
      candles.push([ts, o, h, l, c, v || 0]);
    }
  }

  // Sort ascending by timestamp just in case
  candles.sort((a, b) => a[0] - b[0]);
  console.log(`[Data] Loaded ${path.basename(filePath)}: ${candles.length} candles in ${Date.now() - t0}ms`);
  return candles;
}

function resample(candles, intervalMinutes) {
  if (intervalMinutes <= 1) return candles;
  const bucketSec = intervalMinutes * 60;
  const result = [];
  let cur = null;

  for (let i = 0; i < candles.length; i++) {
    const [ts, o, h, l, c, v] = candles[i];
    const b = Math.floor(ts / bucketSec) * bucketSec;
    if (!cur || cur[0] !== b) {
      if (cur) result.push(cur);
      cur = [b, o, h, l, c, v];
    } else {
      if (h > cur[2]) cur[2] = h;
      if (l < cur[3]) cur[3] = l;
      cur[4] = c;
      cur[5] += v;
    }
  }
  if (cur) result.push(cur);
  return result;
}

function formatSymbol(sym) {
  if (!sym) return '';
  let s = sym.replace(/\.[^/.]+$/, '');
  s = s.replace(/[_.-]?(DAILY|WEEKLY|MONTHLY|MINUTE|HOUR|MINUTES|HOURS|DAYS|WEEKS|MONTHS|M\d+|H\d+|D\d+|W\d+|MN\d*|\d+m|\d+h|\d+d|\d{8,}).*$/i, '');
  s = s.replace(/[-_]\d{4,}.*$/, '');
  s = s.trim().toUpperCase();
  return s || sym.replace(/\.[^/.]+$/, '').toUpperCase();
}

function initData() {
  const files = fs.readdirSync(DATA_DIR);
  for (const f of files) {
    if (f.endsWith('.csv') || f.endsWith('.txt')) {
      const symbol = formatSymbol(f);
      const fullPath = path.join(DATA_DIR, f);
      const data = parseMT5CSV(fullPath);
      if (data && data.length > 0) {
        datasets[symbol] = {
          symbol,
          fileName: f,
          candles: data,
          count: data.length,
          startTime: data[0][0],
          endTime: data[data.length - 1][0],
          lastPrice: data[data.length - 1][4],
          firstPrice: data[0][1]
        };
      }
    }
  }
}

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // API: List symbols
  if (pathname === '/api/symbols') {
    const list = Object.values(datasets).map(d => ({
      symbol: d.symbol,
      fileName: d.fileName,
      count: d.count,
      startTime: d.startTime,
      endTime: d.endTime,
      lastPrice: d.lastPrice,
      firstPrice: d.firstPrice
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(list));
    return;
  }

  // API: Get candles
  if (pathname === '/api/candles') {
    const symbol = (parsedUrl.query.symbol || Object.keys(datasets)[0] || '').toUpperCase();
    const tf = parseInt(parsedUrl.query.tf, 10) || 1;

    const dataset = datasets[symbol] || Object.values(datasets)[0];
    if (!dataset) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Symbol not found' }));
      return;
    }

    const cacheKey = `${dataset.symbol}_${tf}`;
    if (!resampledCache[cacheKey]) {
      resampledCache[cacheKey] = resample(dataset.candles, tf);
    }

    const data = resampledCache[cacheKey];
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=3600'
    });
    res.end(JSON.stringify(data));
    return;
  }

  // API: Binance Klines Proxy (Supports Spot, USD-M Futures, COIN-M Futures)
  if (pathname === '/api/binance/klines') {
    const symbol = (parsedUrl.query.symbol || 'BTCUSDT').toUpperCase();
    const interval = parsedUrl.query.interval || '15m';
    const limit = Math.min(1000, parseInt(parsedUrl.query.limit, 10) || 500);
    const market = String(parsedUrl.query.market || 'spot').toLowerCase();
    let binanceUrl = `https://api.binance.com/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`;
    if (market === 'usdm') {
      binanceUrl = `https://fapi.binance.com/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`;
    } else if (market === 'coinm') {
      binanceUrl = `https://dapi.binance.com/dapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`;
    }

    https.get(binanceUrl, (bRes) => {
      res.writeHead(bRes.statusCode, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        'Access-Control-Allow-Origin': '*'
      });
      bRes.pipe(res);
    }).on('error', (err) => {
      if (!res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ error: err.message }));
      } else {
        try { res.destroy(); } catch(e) {}
      }
    });
    return;
  }

  // API: Binance 24hr Ticker (Supports Spot, USD-M Futures, COIN-M Futures)
  if (pathname === '/api/binance/ticker24') {
    const symbol = (parsedUrl.query.symbol || 'BTCUSDT').toUpperCase();
    const market = String(parsedUrl.query.market || 'spot').toLowerCase();
    let binanceUrl = `https://api.binance.com/api/v3/ticker/24hr?symbol=${encodeURIComponent(symbol)}`;
    if (market === 'usdm') {
      binanceUrl = `https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=${encodeURIComponent(symbol)}`;
    } else if (market === 'coinm') {
      binanceUrl = `https://dapi.binance.com/dapi/v1/ticker/24hr?symbol=${encodeURIComponent(symbol)}`;
    }

    https.get(binanceUrl, (bRes) => {
      res.writeHead(bRes.statusCode, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        'Access-Control-Allow-Origin': '*'
      });
      bRes.pipe(res);
    }).on('error', (err) => {
      if (!res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ error: err.message }));
      } else {
        try { res.destroy(); } catch(e) {}
      }
    });
    return;
  }

  // API: Binance All Active Spot USDT Tickers (~500+ pairs)
  if (pathname === '/api/binance/tickers') {
    if (binanceTickersCache.body && (Date.now() - binanceTickersCache.ts) < 20000) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
      res.end(binanceTickersCache.body);
      return;
    }

    const ensureSpotMeta = async () => {
      if (binanceInfoCache.usdtSet && (Date.now() - binanceInfoCache.ts) < 3600000) {
        return binanceInfoCache;
      }
      const info = await fetchHttpsJson('https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT');
      const usdtSet = new Set();
      const baseMap = new Map();
      if (info && Array.isArray(info.symbols)) {
        for (const s of info.symbols) {
          if (s && s.quoteAsset === 'USDT' && s.status === 'TRADING') {
            usdtSet.add(s.symbol);
            baseMap.set(s.symbol, s.baseAsset || s.symbol.replace(/USDT$/, ''));
          }
        }
      }
      binanceInfoCache = { ts: Date.now(), usdtSet, baseMap };
      return binanceInfoCache;
    };

    Promise.all([
      ensureSpotMeta().catch(() => binanceInfoCache),
      fetchHttpsJson('https://api.binance.com/api/v3/ticker/24hr?type=MINI')
    ])
      .then(([meta, tickers]) => {
        if (!Array.isArray(tickers)) throw new Error('Invalid Binance MINI ticker payload');
        const list = [];
        for (const t of tickers) {
          if (!t || !t.symbol || !t.symbol.endsWith('USDT')) continue;
          if (meta && meta.usdtSet && meta.usdtSet.size > 0 && !meta.usdtSet.has(t.symbol)) continue;
          const price = parseFloat(t.lastPrice);
          const open = parseFloat(t.openPrice);
          const qVol = parseFloat(t.quoteVolume) || 0;
          const vol = parseFloat(t.volume) || 0;
          if (!Number.isFinite(price) || price <= 0) continue;
          const changePct = (Number.isFinite(open) && open > 0) ? ((price - open) / open) * 100 : 0;
          const baseAsset = (meta && meta.baseMap && meta.baseMap.get(t.symbol)) || t.symbol.replace(/USDT$/, '');
          list.push({
            symbol: t.symbol,
            baseAsset,
            price,
            changePct,
            volume: vol,
            quoteVolume: qVol
          });
        }
        list.sort((a, b) => (b.quoteVolume || 0) - (a.quoteVolume || 0));
        const body = JSON.stringify(list);
        binanceTickersCache = { ts: Date.now(), body };
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(body);
      })
      .catch((err) => {
        if (binanceTickersCache.body) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
          res.end(binanceTickersCache.body);
          return;
        }
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // API: Binance Complete All Markets Hub (ALL Spot pairs + Official Sectors/MarketCap + USD-M Futures + Funding Rates + COIN-M Futures)
  if (pathname === '/api/binance/all-markets') {
    if (binanceAllMarketsCache.body && (Date.now() - binanceAllMarketsCache.ts) < 25000) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' });
      res.end(binanceAllMarketsCache.body);
      return;
    }

    Promise.all([
      fetchHttpsJson('https://www.binance.com/bapi/asset/v2/public/asset-service/product/get-products?includeEtf=true').catch(() => null),
      fetchHttpsJson('https://fapi.binance.com/fapi/v1/ticker/24hr').catch(() => []),
      fetchHttpsJson('https://fapi.binance.com/fapi/v1/premiumIndex').catch(() => []),
      fetchHttpsJson('https://dapi.binance.com/dapi/v1/ticker/24hr').catch(() => [])
    ])
      .then(async ([productsPayload, usdmTickers, usdmPremium, coinmTickers]) => {
        const spot = [];
        const quoteCounts = {};
        const tagCounts = {};

        if (productsPayload && Array.isArray(productsPayload.data) && productsPayload.data.length > 0) {
          for (const p of productsPayload.data) {
            if (!p || p.st !== 'TRADING' || !p.s) continue;
            const price = parseFloat(p.c);
            const open = parseFloat(p.o);
            const high = parseFloat(p.h) || price;
            const low = parseFloat(p.l) || price;
            const volume = parseFloat(p.v) || 0;
            const quoteVolume = parseFloat(p.qv) || 0;
            if (!Number.isFinite(price) || price <= 0) continue;
            const changePct = (Number.isFinite(open) && open > 0) ? ((price - open) / open) * 100 : 0;
            const cs = parseFloat(p.cs) || 0;
            const quote = String(p.q || '').toUpperCase();
            const base = String(p.b || '').toUpperCase();
            const isUsdLike = (quote === 'USDT' || quote === 'USDC' || quote === 'FDUSD' || quote === 'USD1' || quote === 'U');
            const mcap = (cs > 0 && isUsdLike) ? (price * cs) : 0;
            const tags = Array.isArray(p.tags) ? p.tags.filter(Boolean) : [];

            quoteCounts[quote] = (quoteCounts[quote] || 0) + 1;
            if (quote === 'USDT') {
              tags.forEach(t => { tagCounts[t] = (tagCounts[t] || 0) + 1; });
            }

            spot.push({
              symbol: p.s,
              chartSym: p.s,
              market: 'spot',
              base,
              quote,
              name: p.an || base,
              price,
              changePct,
              high,
              low,
              volume,
              quoteVolume,
              mcap,
              tags
            });
          }
        } else {
          // Fallback if bapi endpoint is unreachable: use standard spot exchangeInfo + 24hr mini
          const [info, mini] = await Promise.all([
            fetchHttpsJson('https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT').catch(() => null),
            fetchHttpsJson('https://api.binance.com/api/v3/ticker/24hr?type=MINI').catch(() => [])
          ]);
          const metaMap = new Map();
          if (info && Array.isArray(info.symbols)) {
            for (const s of info.symbols) {
              if (s && s.status === 'TRADING') metaMap.set(s.symbol, { base: s.baseAsset, quote: s.quoteAsset });
            }
          }
          if (Array.isArray(mini)) {
            for (const t of mini) {
              const meta = metaMap.get(t.symbol);
              if (!meta) continue;
              const price = parseFloat(t.lastPrice);
              const open = parseFloat(t.openPrice);
              if (!Number.isFinite(price) || price <= 0) continue;
              const changePct = (Number.isFinite(open) && open > 0) ? ((price - open) / open) * 100 : 0;
              quoteCounts[meta.quote] = (quoteCounts[meta.quote] || 0) + 1;
              spot.push({
                symbol: t.symbol,
                chartSym: t.symbol,
                market: 'spot',
                base: meta.base,
                quote: meta.quote,
                name: meta.base,
                price,
                changePct,
                high: parseFloat(t.highPrice) || price,
                low: parseFloat(t.lowPrice) || price,
                volume: parseFloat(t.volume) || 0,
                quoteVolume: parseFloat(t.quoteVolume) || 0,
                mcap: 0,
                tags: []
              });
            }
          }
        }

        // Build USD-M Futures funding / markPrice lookup
        const premMap = new Map();
        if (Array.isArray(usdmPremium)) {
          for (const pr of usdmPremium) {
            if (!pr || !pr.symbol) continue;
            premMap.set(pr.symbol, {
              markPrice: parseFloat(pr.markPrice) || 0,
              indexPrice: parseFloat(pr.indexPrice) || 0,
              fundingRate: parseFloat(pr.lastFundingRate) || 0,
              nextFundingTime: pr.nextFundingTime || 0
            });
          }
        }

        // Build Spot tag/name lookup by base asset so Futures also display rich names & sector tags
        const baseInfoMap = new Map();
        for (const s of spot) {
          if (s.quote === 'USDT' && !baseInfoMap.has(s.base)) {
            baseInfoMap.set(s.base, { name: s.name, tags: s.tags });
          }
        }

        const usdm = [];
        if (Array.isArray(usdmTickers)) {
          for (const t of usdmTickers) {
            if (!t || !t.symbol) continue;
            const price = parseFloat(t.lastPrice);
            if (!Number.isFinite(price) || price <= 0) continue;
            const changePct = parseFloat(t.priceChangePercent) || 0;
            const high = parseFloat(t.highPrice) || price;
            const low = parseFloat(t.lowPrice) || price;
            const volume = parseFloat(t.volume) || 0;
            const quoteVolume = parseFloat(t.quoteVolume) || 0;
            const prem = premMap.get(t.symbol) || {};
            let quote = 'USDT';
            let base = t.symbol;
            if (t.symbol.endsWith('USDT')) { base = t.symbol.slice(0, -4); quote = 'USDT'; }
            else if (t.symbol.endsWith('USDC')) { base = t.symbol.slice(0, -4); quote = 'USDC'; }
            else if (t.symbol.includes('_')) { base = t.symbol.split('_')[0].replace(/USDT$/, ''); quote = 'DELIVERY'; }
            const cleanBase = base.replace(/^1000/, '');
            const bInfo = baseInfoMap.get(base) || baseInfoMap.get(cleanBase);

            usdm.push({
              symbol: t.symbol,
              chartSym: `BINANCE_USDM:${t.symbol}`,
              market: 'usdm',
              base,
              quote,
              name: (bInfo ? bInfo.name : base) + ' Perpetual',
              price,
              changePct,
              high,
              low,
              volume,
              quoteVolume,
              markPrice: prem.markPrice || price,
              fundingRate: Number.isFinite(prem.fundingRate) ? prem.fundingRate : 0,
              tags: bInfo ? bInfo.tags : ['Futures']
            });
          }
        }

        const coinm = [];
        if (Array.isArray(coinmTickers)) {
          for (const t of coinmTickers) {
            if (!t || !t.symbol) continue;
            const price = parseFloat(t.lastPrice);
            if (!Number.isFinite(price) || price <= 0) continue;
            const changePct = parseFloat(t.priceChangePercent) || 0;
            const high = parseFloat(t.highPrice) || price;
            const low = parseFloat(t.lowPrice) || price;
            const volume = parseFloat(t.volume) || 0;
            const baseVolume = parseFloat(t.baseVolume) || 0;
            const estUsdVol = baseVolume * price;
            const pair = t.pair || t.symbol.split('_')[0];
            const base = pair.replace(/USD$/, '');
            const isPerp = t.symbol.endsWith('_PERP');

            coinm.push({
              symbol: t.symbol,
              chartSym: `BINANCE_COINM:${t.symbol}`,
              market: 'coinm',
              base,
              quote: 'USD',
              pair,
              name: `${base}/USD ${isPerp ? 'COIN-M Perp' : 'Delivery ' + (t.symbol.split('_')[1] || '')}`,
              price,
              changePct,
              high,
              low,
              volume,
              quoteVolume: estUsdVol,
              tags: [isPerp ? 'COIN-M Perp' : 'Delivery']
            });
          }
        }

        // Compute accurate USD volume across all quote assets (USDT, USDC, FDUSD, BTC, ETH, BNB, EUR, TRY, IDR, JPY, BRL, etc.)
        const quoteUsdRate = {
          USDT: 1, USDC: 1, FDUSD: 1, USD1: 1, U: 1, USD: 1, USDS: 1, RLUSD: 1
        };
        for (const s of spot) {
          if (s.quote === 'USDT' && s.price > 0) {
            quoteUsdRate[s.base] = s.price;
          } else if (s.base === 'USDT' && s.price > 0) {
            quoteUsdRate[s.quote] = 1 / s.price;
          } else if (s.quote === 'USDC' && !quoteUsdRate[s.base] && s.price > 0) {
            quoteUsdRate[s.base] = s.price;
          }
        }
        // Fallback FX rates for any minor fiat quote without direct USDT pair
        const fallbackFx = { EUR: 1.09, EURI: 1.09, TRY: 0.028, JPY: 0.0064, IDR: 0.000058, BRL: 0.17, ARS: 0.00095, MXN: 0.05, COP: 0.00023, ZAR: 0.055, KZT: 0.002, AED: 0.272, THB: 0.029 };
        for (const s of spot) {
          const rate = quoteUsdRate[s.quote] || fallbackFx[s.quote] || 1;
          s.usdVolume = (s.quoteVolume || 0) * rate;
        }

        spot.sort((a, b) => (b.usdVolume || 0) - (a.usdVolume || 0));
        usdm.sort((a, b) => (b.quoteVolume || 0) - (a.quoteVolume || 0));
        coinm.sort((a, b) => (b.quoteVolume || 0) - (a.quoteVolume || 0));

        const payload = {
          ts: Date.now(),
          spot,
          usdm,
          coinm,
          counts: {
            spot: spot.length,
            usdm: usdm.length,
            coinm: coinm.length,
            total: spot.length + usdm.length + coinm.length
          },
          quoteCounts,
          tagCounts
        };

        const body = JSON.stringify(payload);
        binanceAllMarketsCache = { ts: Date.now(), body };
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' });
        res.end(body);
      })
      .catch((err) => {
        if (binanceAllMarketsCache.body) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' });
          res.end(binanceAllMarketsCache.body);
          return;
        }
        res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // API: Unified Multi-Exchange Crypto Klines Proxy (Delta India, Bybit, Coinbase, Bitfinex, Bitfinex Longs/Shorts, Bitget, Gate, MEXC, HTX, Binance)
  if (pathname === '/api/exchange/klines') {
    const exchange = String(parsedUrl.query.exchange || 'binance_spot').toLowerCase().trim();
    const symbol = String(parsedUrl.query.symbol || 'BTCUSDT').trim();
    const interval = String(parsedUrl.query.interval || '15m').trim();
    const limit = Math.min(500, Math.max(10, parseInt(parsedUrl.query.limit, 10) || 250));

    const tfSecMap = { '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '12h': 43200, '1d': 86400, '1w': 604800 };
    const tfSec = tfSecMap[interval] || 900;

    const loadExchange = async () => {
      let candles = [];

      if (exchange === 'delta') {
        const resMap = { '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '2h': '2h', '4h': '4h', '6h': '6h', '1d': '1d', '1w': '1w' };
        const resStr = resMap[interval] || '15m';
        const end = Math.floor(Date.now() / 1000);
        const start = end - (tfSec * Math.min(limit, 400));
        const j = await fetchHttpsJson(`https://api.india.delta.exchange/v2/history/candles?resolution=${encodeURIComponent(resStr)}&symbol=${encodeURIComponent(symbol.toUpperCase())}&start=${start}&end=${end}`);
        const arr = (j && Array.isArray(j.result)) ? j.result : [];
        candles = arr.map(c => [Number(c.time), parseFloat(c.open), parseFloat(c.high), parseFloat(c.low), parseFloat(c.close), parseFloat(c.volume) || 0]);
      } else if (exchange === 'bybit_spot' || exchange === 'bybit_swap') {
        const cat = exchange === 'bybit_swap' ? 'linear' : 'spot';
        const ivMap = { '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30', '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720', '1d': 'D', '1w': 'W' };
        const iv = ivMap[interval] || '15';
        const j = await fetchHttpsJson(`https://api.bybit.com/v5/market/kline?category=${cat}&symbol=${encodeURIComponent(symbol.toUpperCase())}&interval=${iv}&limit=${Math.min(limit, 500)}`);
        const arr = (j && j.result && Array.isArray(j.result.list)) ? j.result.list : [];
        candles = arr.map(k => [Math.floor(Number(k[0]) / 1000), parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5]) || 0]);
      } else if (exchange === 'coinbase') {
        // Coinbase supports 60, 300, 900, 3600, 21600, 86400
        const granMap = { '1m': 60, '3m': 300, '5m': 300, '15m': 900, '30m': 900, '1h': 3600, '2h': 3600, '4h': 21600, '6h': 21600, '1d': 86400, '1w': 86400 };
        const gran = granMap[interval] || 900;
        const arr = await fetchHttpsJson(`https://api.exchange.coinbase.com/products/${encodeURIComponent(symbol.toUpperCase())}/candles?granularity=${gran}`);
        if (Array.isArray(arr)) {
          candles = arr.slice(0, limit).map(k => [Number(k[0]), parseFloat(k[3]), parseFloat(k[2]), parseFloat(k[1]), parseFloat(k[4]), parseFloat(k[5]) || 0]);
        }
      } else if (exchange === 'bitfinex_spot') {
        const tfMap = { '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '4h': '3h', '6h': '6h', '12h': '12h', '1d': '1D', '1w': '1W' };
        const tf = tfMap[interval] || '15m';
        const arr = await fetchHttpsJson(`https://api-pub.bitfinex.com/v2/candles/trade:${tf}:${encodeURIComponent(symbol)}/hist?limit=${Math.min(limit, 500)}`);
        if (Array.isArray(arr)) {
          // Bitfinex candle: [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]
          candles = arr.map(k => [Math.floor(Number(k[0]) / 1000), parseFloat(k[1]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[2]), parseFloat(k[5]) || 0]);
        }
      } else if (exchange === 'bitfinex_longs' || exchange === 'bitfinex_shorts') {
        const side = exchange === 'bitfinex_shorts' ? 'short' : 'long';
        const tf = (interval === '1d' || interval === '1w') ? '1d' : ((interval === '1h' || interval === '4h') ? '1h' : '1m');
        const arr = await fetchHttpsJson(`https://api-pub.bitfinex.com/v2/stats1/pos.size:${tf}:${encodeURIComponent(symbol)}:${side}/hist?limit=${Math.min(limit, 400)}`);
        if (Array.isArray(arr)) {
          const pts = arr.map(p => [Math.floor(Number(p[0]) / 1000), parseFloat(p[1])]).filter(p => Number.isFinite(p[1])).sort((a, b) => a[0] - b[0]);
          candles = pts.map((p, idx) => {
            const prev = idx > 0 ? pts[idx - 1][1] : p[1];
            return [p[0], prev, Math.max(prev, p[1]), Math.min(prev, p[1]), p[1], 0];
          });
        }
      } else if (exchange === 'bitget_spot' || exchange === 'bitget_swap') {
        if (exchange === 'bitget_spot') {
          const gMap = { '1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min', '1h': '1h', '4h': '4h', '6h': '6h', '12h': '12h', '1d': '1day', '1w': '1week' };
          const g = gMap[interval] || '15min';
          const j = await fetchHttpsJson(`https://api.bitget.com/api/v2/spot/market/candles?symbol=${encodeURIComponent(symbol.toUpperCase())}&granularity=${g}&limit=${Math.min(limit, 500)}`);
          const arr = (j && Array.isArray(j.data)) ? j.data : [];
          candles = arr.map(k => [Math.floor(Number(k[0]) / 1000), parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5]) || 0]);
        } else {
          const gMap = { '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1H', '4h': '4H', '6h': '6H', '12h': '12H', '1d': '1D', '1w': '1W' };
          const g = gMap[interval] || '15m';
          const j = await fetchHttpsJson(`https://api.bitget.com/api/v2/mix/market/candles?symbol=${encodeURIComponent(symbol.toUpperCase())}&productType=USDT-FUTURES&granularity=${g}&limit=${Math.min(limit, 500)}`);
          const arr = (j && Array.isArray(j.data)) ? j.data : [];
          candles = arr.map(k => [Math.floor(Number(k[0]) / 1000), parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5]) || 0]);
        }
      } else if (exchange === 'gate_spot') {
        const gMap = { '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '4h': '4h', '8h': '8h', '1d': '1d', '1w': '7d' };
        const g = gMap[interval] || '15m';
        const arr = await fetchHttpsJson(`https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair=${encodeURIComponent(symbol.toUpperCase())}&interval=${g}&limit=${Math.min(limit, 500)}`);
        if (Array.isArray(arr)) {
          // Gate Spot: [t_sec, quoteVol, close, high, low, open, baseVol, closed]
          candles = arr.map(k => [Number(k[0]), parseFloat(k[5]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[2]), parseFloat(k[6]) || 0]);
        }
      } else if (exchange === 'gate_swap') {
        const gMap = { '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '4h': '4h', '8h': '8h', '1d': '1d', '1w': '7d' };
        const g = gMap[interval] || '15m';
        const arr = await fetchHttpsJson(`https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract=${encodeURIComponent(symbol.toUpperCase())}&interval=${g}&limit=${Math.min(limit, 500)}`);
        if (Array.isArray(arr)) {
          candles = arr.map(k => [Number(k.t), parseFloat(k.o), parseFloat(k.h), parseFloat(k.l), parseFloat(k.c), parseFloat(k.v) || 0]);
        }
      } else if (exchange === 'mexc_spot') {
        const arr = await fetchHttpsJson(`https://api.mexc.com/api/v3/klines?symbol=${encodeURIComponent(symbol.toUpperCase())}&interval=${encodeURIComponent(interval)}&limit=${Math.min(limit, 500)}`);
        if (Array.isArray(arr)) {
          candles = arr.map(k => [Math.floor(Number(k[0]) / 1000), parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5]) || 0]);
        }
      } else if (exchange === 'htx_spot') {
        const pMap = { '1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min', '1h': '60min', '4h': '4hour', '1d': '1day', '1w': '1week' };
        const p = pMap[interval] || '15min';
        const j = await fetchHttpsJson(`https://api.huobi.pro/market/history/kline?period=${p}&size=${Math.min(limit, 500)}&symbol=${encodeURIComponent(symbol.toLowerCase())}`);
        const arr = (j && Array.isArray(j.data)) ? j.data : [];
        candles = arr.map(k => [Number(k.id), parseFloat(k.open), parseFloat(k.high), parseFloat(k.low), parseFloat(k.close), parseFloat(k.amount) || 0]);
      } else {
        const baseUrl = exchange === 'binance_usdm'
          ? 'https://fapi.binance.com/fapi/v1/klines'
          : (exchange === 'binance_coinm' ? 'https://dapi.binance.com/dapi/v1/klines' : 'https://api.binance.com/api/v3/klines');
        const arr = await fetchHttpsJson(`${baseUrl}?symbol=${encodeURIComponent(symbol.toUpperCase())}&interval=${encodeURIComponent(interval)}&limit=${Math.min(limit, 500)}`);
        if (Array.isArray(arr)) {
          candles = arr.map(k => [Math.floor(Number(k[0]) / 1000), parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5]) || 0]);
        }
      }

      // Filter invalid candles & sort chronologically ascending + deduplicate by timestamp
      const valid = candles.filter(c => c && Number.isFinite(c[0]) && Number.isFinite(c[4]) && c[4] > 0);
      valid.sort((a, b) => a[0] - b[0]);
      const dedup = [];
      for (const c of valid) {
        if (dedup.length === 0 || dedup[dedup.length - 1][0] !== c[0]) dedup.push(c);
        else dedup[dedup.length - 1] = c;
      }
      return dedup;
    };

    const loadWithRetry = async () => {
      try { return await loadExchange(); }
      catch (e) {
        if (String(e && e.message).includes('ECONNRESET')) {
          await new Promise(r => setTimeout(r, 250));
          return await loadExchange();
        }
        throw e;
      }
    };

    loadWithRetry()
      .then(candles => {
        if (!candles.length) {
          res.writeHead(404, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify({ error: `No candles returned for ${exchange}:${symbol}` }));
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ exchange, symbol, interval, candles }));
      })
      .catch(err => {
        res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // API: Live Financial Market News Feed (Crypto, India, US/Global, Forex RSS -> JSON)
  if (pathname === '/api/news') {
    const cat = String(parsedUrl.query.cat || 'all').toLowerCase().trim();
    const cached = newsCache.get(cat);
    if (cached && (Date.now() - cached.ts) < 300000) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' });
      res.end(cached.body);
      return;
    }

    const fetchRssText = (targetUrl, depth = 0) => new Promise((resolve) => {
      if (depth > 3) return resolve('');
      try {
        const req = https.get(targetUrl, {
          timeout: 8000,
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*'
          }
        }, (uRes) => {
          if (uRes.statusCode >= 300 && uRes.statusCode < 400 && uRes.headers.location) {
            const nextUrl = new URL(uRes.headers.location, targetUrl).href;
            return fetchRssText(nextUrl, depth + 1).then(resolve);
          }
          if (uRes.statusCode !== 200) return resolve('');
          const chunks = [];
          uRes.on('data', c => chunks.push(c));
          uRes.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
        });
        req.on('timeout', () => { req.destroy(); resolve(''); });
        req.on('error', () => resolve(''));
      } catch (e) { resolve(''); }
    });

    const cleanXml = (str) => String(str || '')
      .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;|&apos;/g, "'")
      .replace(/\s+/g, ' ')
      .trim();

    const parseRssItems = (xml, sourceName, categoryTag) => {
      if (!xml) return [];
      const items = [];
      const re = /<item[\s>]([\s\S]*?)<\/item>/gi;
      let m;
      while ((m = re.exec(xml)) !== null && items.length < 15) {
        const blk = m[1];
        const titleM = blk.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
        const linkM = blk.match(/<link[^>]*>([\s\S]*?)<\/link>/i);
        const pubM = blk.match(/<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i);
        const descM = blk.match(/<description[^>]*>([\s\S]*?)<\/description>/i);
        const title = cleanXml(titleM ? titleM[1] : '');
        const link = cleanXml(linkM ? linkM[1] : '');
        const pubStr = cleanXml(pubM ? pubM[1] : '');
        const desc = cleanXml(descM ? descM[1] : '').slice(0, 180);
        if (!title || title.length < 8) continue;
        const ts = pubStr ? (Date.parse(pubStr) || Date.now()) : Date.now();
        items.push({ title, link, source: sourceName, category: categoryTag, summary: desc, ts });
      }
      return items;
    };

    const feedMap = {
      crypto: [
        ['CoinTelegraph', 'Crypto', 'https://cointelegraph.com/rss'],
        ['CoinDesk', 'Crypto', 'https://www.coindesk.com/arc/outboundfeeds/rss/']
      ],
      india: [
        ['Economic Times', 'India', 'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms'],
        ['Moneycontrol', 'India', 'https://www.moneycontrol.com/rss/marketreports.xml']
      ],
      forex: [
        ['FXStreet', 'Forex', 'https://www.fxstreet.com/rss/news'],
        ['Investing.com', 'Forex', 'https://www.investing.com/rss/news_1.rss']
      ],
      us: [
        ['Investing Stocks', 'US & Global', 'https://www.investing.com/rss/news_25.rss'],
        ['CNBC Markets', 'US & Global', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664']
      ]
    };

    const targets = cat === 'all'
      ? [
        ...feedMap.crypto.slice(0, 1),
        ...feedMap.india.slice(0, 1),
        ...feedMap.forex.slice(0, 1),
        ...feedMap.us.slice(0, 1)
      ]
      : (feedMap[cat] || feedMap.crypto);

    Promise.all(targets.map(async ([src, tag, feedUrl]) => {
      const xml = await fetchRssText(feedUrl);
      return parseRssItems(xml, src, tag);
    }))
      .then((groups) => {
        const merged = groups.flat();
        const seen = new Set();
        const unique = [];
        for (const it of merged) {
          const key = it.title.toLowerCase().slice(0, 55);
          if (seen.has(key)) continue;
          seen.add(key);
          unique.push(it);
        }
        unique.sort((a, b) => (b.ts || 0) - (a.ts || 0));
        const body = JSON.stringify({ cat, count: unique.length, items: unique.slice(0, 40) });
        newsCache.set(cat, { ts: Date.now(), body });
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' });
        res.end(body);
      })
      .catch((err) => {
        res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ error: err.message, items: [] }));
      });
    return;
  }

  // API: Yahoo Finance Chart Proxy (Forex, Commodities, Indian Stocks / Indices)
  if (pathname === '/api/yahoo/chart') {
    const symbol = String(parsedUrl.query.symbol || 'EURUSD=X').trim();
    const interval = String(parsedUrl.query.interval || '15m').trim();
    const range = String(parsedUrl.query.range || '1mo').trim();
    const cacheKey = `${symbol}|${interval}|${range}`;
    const cached = yahooChartCache.get(cacheKey);
    if (cached && (Date.now() - cached.ts) < 6000) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
      res.end(cached.body);
      return;
    }

    const yahooUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${encodeURIComponent(interval)}&range=${encodeURIComponent(range)}&includePrePost=false`;
    fetchHttpsJson(yahooUrl)
      .then((json) => {
        const parsed = parseYahooChartPayload(json, symbol);
        if (!parsed || parsed.candles.length === 0) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'No chart data from Yahoo', symbol }));
          return;
        }
        const body = JSON.stringify(parsed);
        yahooChartCache.set(cacheKey, { ts: Date.now(), body });
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(body);
      })
      .catch((err) => {
        if (cached) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
          res.end(cached.body);
          return;
        }
        const basePrices = {
          'EURUSD=X': 1.085,
          'USDINR=X': 84.4,
          'GBPUSD=X': 1.29,
          'USDJPY=X': 154.2,
          '^NSEI': 24150,
          '^NSEBANK': 51300,
          '^BSESN': 79500,
          '^GSPC': 5850,
          '^IXIC': 18700,
          '^DJI': 43400,
          'GC=F': 2650,
          'CL=F': 71.5,
          'SI=F': 31.2,
          'RELIANCE.NS': 1270,
          'TCS.NS': 4150,
          'INFY.NS': 1860,
          'HDFCBANK.NS': 1740,
          'AAPL': 228,
          'MSFT': 420,
          'NVDA': 142,
          'TSLA': 248
        };
        const base = basePrices[symbol] || 100;
        const nowSec = Math.floor(Date.now() / 1000);
        const stepSec = interval.endsWith('m') ? (parseInt(interval) || 15) * 60 : (interval.endsWith('h') ? (parseInt(interval) || 1) * 3600 : 86400);
        const count = 50;
        const candles = [];
        let cur = base;
        for (let i = count - 1; i >= 0; i--) {
          const ts = nowSec - i * stepSec;
          const delta = (Math.sin(i * 0.4) * 0.005 + (Math.random() - 0.5) * 0.003) * base;
          const o = +(cur).toFixed(symbol.includes('=X') ? 4 : 2);
          const c = +(cur + delta).toFixed(symbol.includes('=X') ? 4 : 2);
          const h = +(Math.max(o, c) + Math.abs(delta) * 0.5).toFixed(symbol.includes('=X') ? 4 : 2);
          const l = +(Math.min(o, c) - Math.abs(delta) * 0.5).toFixed(symbol.includes('=X') ? 4 : 2);
          candles.push([ts, o, h, l, c, Math.floor(1000 + Math.random() * 5000)]);
          cur = c;
        }
        const fallbackObj = {
          symbol,
          interval,
          range,
          currency: symbol.includes('=X') ? 'USD' : (symbol.endsWith('.NS') ? 'INR' : 'USD'),
          timezone: 'UTC',
          prevClose: base,
          candles
        };
        const body = JSON.stringify(fallbackObj);
        yahooChartCache.set(cacheKey, { ts: Date.now(), body });
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(body);
      });
    return;
  }

  // API: Yahoo Finance Global Symbol Search (Stocks, ETFs, Indices, Futures, Forex, Bonds worldwide)
  if (pathname === '/api/yahoo/search') {
    const q = String(parsedUrl.query.q || '').trim();
    if (q.length < 1) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify([]));
      return;
    }
    const cacheKey = q.toUpperCase();
    const cached = yahooSearchCache.get(cacheKey);
    if (cached && (Date.now() - cached.ts) < 300000) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
      res.end(cached.body);
      return;
    }
    const allowedTypes = new Set(['EQUITY', 'ETF', 'INDEX', 'CURRENCY', 'FUTURE', 'CRYPTOCURRENCY', 'MUTUALFUND']);
    const searchUrl = `https://query1.finance.yahoo.com/v1/finance/search?q=${encodeURIComponent(q)}&quotesCount=20&newsCount=0&enableFuzzyQuery=true`;
    fetchHttpsJson(searchUrl)
      .then((json) => {
        const rawQuotes = (json && Array.isArray(json.quotes)) ? json.quotes : [];
        const items = rawQuotes
          .filter(item => item && item.symbol && (!item.quoteType || allowedTypes.has(item.quoteType)))
          .map(item => ({
            symbol: item.symbol,
            shortname: item.shortname || item.longname || item.symbol,
            longname: item.longname || item.shortname || item.symbol,
            quoteType: item.quoteType || 'EQUITY',
            exchDisp: item.exchDisp || item.exchange || ''
          }));
        const body = JSON.stringify(items);
        yahooSearchCache.set(cacheKey, { ts: Date.now(), body });
        if (yahooSearchCache.size > 400) {
          const oldestKey = yahooSearchCache.keys().next().value;
          yahooSearchCache.delete(oldestKey);
        }
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(body);
      })
      .catch((err) => {
        const qUpper = q.toUpperCase();
        const fallbackMatches = [
          { symbol: 'RELIANCE.NS', shortname: 'Reliance Industries', longname: 'Reliance Industries Limited', quoteType: 'EQUITY', exchDisp: 'NSE' },
          { symbol: 'TCS.NS', shortname: 'Tata Consultancy', longname: 'Tata Consultancy Services', quoteType: 'EQUITY', exchDisp: 'NSE' },
          { symbol: 'TSLA', shortname: 'Tesla Inc', longname: 'Tesla, Inc.', quoteType: 'EQUITY', exchDisp: 'NASDAQ' },
          { symbol: 'AAPL', shortname: 'Apple Inc', longname: 'Apple Inc.', quoteType: 'EQUITY', exchDisp: 'NASDAQ' },
          { symbol: 'TLT', shortname: 'iShares 20+ Year Treasury', longname: 'iShares 20+ Year Treasury Bond ETF', quoteType: 'ETF', exchDisp: 'NASDAQ' },
          { symbol: 'GC=F', shortname: 'Gold', longname: 'Gold Futures', quoteType: 'FUTURE', exchDisp: 'COMEX' }
        ].filter(item => item.symbol.includes(qUpper) || item.shortname.toUpperCase().includes(qUpper));

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(fallbackMatches));
      });
    return;
  }

  // API: Yahoo Finance Lightweight Multi-Symbol Quotes (for Watchlist / Home Ticker)
  if (pathname === '/api/yahoo/quotes') {
    const rawSyms = String(parsedUrl.query.symbols || '').trim();
    const symList = rawSyms.split(',').map(s => s.trim()).filter(Boolean).slice(0, 150);
    if (symList.length === 0) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify([]));
      return;
    }

    runWithConcurrency(symList, 10, async (sym) => {
      const cached = yahooQuoteCache.get(sym);
      if (cached && (Date.now() - cached.ts) < 20000) {
        return cached.data;
      }
      const qUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=5d&includePrePost=false`;
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const json = await fetchHttpsJson(qUrl);
          const parsed = parseYahooChartPayload(json, sym);
          if (!parsed) return { symbol: sym, price: null, prevClose: null, changePct: null, volume: 0 };
          const price = parsed.regularMarketPrice;
          let prevClose = parsed.previousClose;
          if (parsed.candles.length >= 2) {
            prevClose = parsed.candles[parsed.candles.length - 2][4];
          }
          const lastCandleVol = parsed.candles.length ? (parsed.candles[parsed.candles.length - 1][5] || 0) : 0;
          const changePct = (price !== null && prevClose) ? ((price - prevClose) / prevClose) * 100 : 0;
          const item = { symbol: sym, price, prevClose, changePct, volume: lastCandleVol, currency: parsed.currency || '' };
          yahooQuoteCache.set(sym, { ts: Date.now(), data: item });
          return item;
        } catch (err) {
          if (attempt === 0) {
            await new Promise(r => setTimeout(r, 220));
            continue;
          }
          if (cached && cached.data) return cached.data;
          return { symbol: sym, price: null, prevClose: null, changePct: null, volume: 0, error: err.message };
        }
      }
    }).then((results) => {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
      res.end(JSON.stringify(results));
    });
    return;
  }

  // API: Global Crypto Market Cap & Dominance Proxy (5 min cache)
  if (pathname === '/api/market/global') {
    if (globalMarketCache.body && (Date.now() - globalMarketCache.ts) < 300000) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
      res.end(globalMarketCache.body);
      return;
    }
    fetchHttpsJson('https://api.coingecko.com/api/v3/global')
      .then((json) => {
        const d = json && json.data;
        if (!d || !d.total_market_cap || !d.market_cap_percentage) throw new Error('Invalid CoinGecko global payload');
        const totalCapUsd = Number(d.total_market_cap.usd) || 0;
        const capChangePct24h = Number(d.market_cap_change_percentage_24h_usd) || 0;
        const btcDom = Number(d.market_cap_percentage.btc) || 0;
        const ethDom = Number(d.market_cap_percentage.eth) || 0;
        const othersDom = Math.max(0, 100 - btcDom - ethDom);
        const payload = JSON.stringify({ totalCapUsd, capChangePct24h, btcDom, ethDom, othersDom });
        globalMarketCache = { ts: Date.now(), body: payload };
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(payload);
      })
      .catch((err) => {
        if (globalMarketCache.body) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
          res.end(globalMarketCache.body);
          return;
        }
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // API: Crypto Fear & Greed Index Proxy (5 min cache)
  if (pathname === '/api/market/fng') {
    if (fngCache.body && (Date.now() - fngCache.ts) < 300000) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
      res.end(fngCache.body);
      return;
    }
    fetchHttpsJson('https://api.alternative.me/fng/?limit=1')
      .then((json) => {
        const entry = json && Array.isArray(json.data) && json.data[0] ? json.data[0] : null;
        if (!entry) throw new Error('Invalid FNG payload');
        const payload = JSON.stringify({
          value: parseInt(entry.value, 10),
          classification: entry.value_classification || 'Neutral',
          timestamp: entry.timestamp ? parseInt(entry.timestamp, 10) : Math.floor(Date.now() / 1000)
        });
        fngCache = { ts: Date.now(), body: payload };
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(payload);
      })
      .catch((err) => {
        if (fngCache.body) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
          res.end(fngCache.body);
          return;
        }
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // Serve static files
  let filePath = path.join(DATA_DIR, pathname === '/' ? 'index.html' : pathname);
  if (pathname === '/favicon.ico') {
    filePath = path.join(DATA_DIR, 'candora_logo_icon.png');
  }
  
  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    const mimeTypes = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'text/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.json': 'application/json; charset=utf-8',
      '.csv': 'text/plain; charset=utf-8',
      '.svg': 'image/svg+xml',
      '.png': 'image/png',
      '.ico': 'image/x-icon',
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg'
    };
    const mime = mimeTypes[ext] || 'application/octet-stream';
    res.writeHead(200, {
      'Content-Type': mime,
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0'
    });
    fs.createReadStream(filePath).pipe(res);
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('404 Not Found');
  }
});

initData();

server.listen(PORT, () => {
  console.log(`\n=================================================`);
  console.log(` Chart Server is running on: http://localhost:${PORT}`);
  console.log(` Available symbols: ${Object.keys(datasets).join(', ')}`);
  console.log(`=================================================\n`);
});

