interface Env {
  DB: D1Database;
  RAW_TOPIC: string;
  COLLECTOR_MODE: string;
  CONFLUENT_REST_ENDPOINT: string;
  CONFLUENT_CLUSTER_ID: string;
  KAFKA_API_KEY: string;
  KAFKA_API_SECRET: string;
  DASHBOARD_INGEST_TOKEN: string;
  EIA_API_KEY: string;
  FRED_API_KEY: string;
}

type EconomicEvent = {
  source: string;
  metric: string;
  label: string;
  value: number;
  unit: string;
  observed_at: string;
  collected_at: string;
  dimensions: Record<string, string | number | boolean | null>;
};

const jsonHeaders = { "content-type": "application/json; charset=utf-8" };

function nowIso(): string {
  return new Date().toISOString();
}

function asIso(value: string | number | undefined | null): string {
  if (value === undefined || value === null || value === "") return nowIso();
  if (typeof value === "number") {
    return new Date(value > 10_000_000_000 ? value : value * 1000).toISOString();
  }
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? nowIso() : d.toISOString();
}

function authOk(request: Request, env: Env): boolean {
  const header = request.headers.get("authorization") || "";
  return header === `Bearer ${env.DASHBOARD_INGEST_TOKEN}`;
}

async function fetchJson(url: string, init?: RequestInit): Promise<any> {
  const response = await fetch(url, {
    ...init,
    headers: {
      "user-agent": "confluent-economic-pulse/0.1",
      "accept": "application/json",
      ...(init?.headers || {})
    }
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`HTTP ${response.status} from ${url}: ${body.slice(0, 300)}`);
  }
  return response.json();
}

function basicAuth(key: string, secret: string): string {
  return btoa(`${key}:${secret}`);
}

async function produce(env: Env, event: EconomicEvent): Promise<void> {
  const base = env.CONFLUENT_REST_ENDPOINT.replace(/\/$/, "");
  const url =
    `${base}/kafka/v3/clusters/${encodeURIComponent(env.CONFLUENT_CLUSTER_ID)}` +
    `/topics/${encodeURIComponent(env.RAW_TOPIC)}/records`;

  const payload = {
    value: {
      type: "JSON",
      data: event
    }
  };

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "authorization": `Basic ${basicAuth(env.KAFKA_API_KEY, env.KAFKA_API_SECRET)}`,
      "content-type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const body = await response.text();
  if (!response.ok) throw new Error(`Confluent REST ${response.status}: ${body}`);

  // The Produce API can return HTTP 200 with a per-record error code.
  try {
    const report = JSON.parse(body);
    if (report.error_code && report.error_code !== 200) {
      throw new Error(`Confluent delivery error ${report.error_code}: ${body}`);
    }
  } catch (err) {
    if (err instanceof SyntaxError) return;
    throw err;
  }
}

async function emitAll(env: Env, events: EconomicEvent[]): Promise<void> {
  for (const event of events) await produce(env, event);
}

async function collectCoinGecko(env: Env): Promise<EconomicEvent[]> {
  const url =
    "https://api.coingecko.com/api/v3/simple/price" +
    "?ids=bitcoin,ethereum" +
    "&vs_currencies=usd" +
    "&include_market_cap=true" +
    "&include_24hr_vol=true" +
    "&include_24hr_change=true" +
    "&include_last_updated_at=true";

  const data = await fetchJson(url);
  const collected = nowIso();
  const out: EconomicEvent[] = [];

  for (const [id, label] of [["bitcoin", "Bitcoin"], ["ethereum", "Ethereum"]] as const) {
    const row = data[id];
    if (!row) continue;
    const observed = asIso(row.last_updated_at);

    out.push({
      source: "coingecko",
      metric: `crypto.${id}.usd`,
      label: `${label} price`,
      value: Number(row.usd),
      unit: "USD",
      observed_at: observed,
      collected_at: collected,
      dimensions: {
        asset: id,
        market_cap_usd: Number(row.usd_market_cap ?? 0),
        volume_24h_usd: Number(row.usd_24h_vol ?? 0),
        change_24h_pct: Number(row.usd_24h_change ?? 0)
      }
    });
  }
  return out;
}

async function collectNBP(env: Env): Promise<EconomicEvent[]> {
  const currencies = ["USD", "EUR", "GBP", "CHF"];
  const collected = nowIso();
  const out: EconomicEvent[] = [];

  for (const code of currencies) {
    const data = await fetchJson(`https://api.nbp.pl/api/exchangerates/rates/A/${code}/?format=json`);
    const rate = data?.rates?.[0];
    if (!rate) continue;
    out.push({
      source: "nbp",
      metric: `fx.${code.toLowerCase()}_pln`,
      label: `${code} / PLN`,
      value: Number(rate.mid),
      unit: "PLN",
      observed_at: asIso(`${rate.effectiveDate}T00:00:00Z`),
      collected_at: collected,
      dimensions: { currency: code, table: data.table || "A" }
    });
  }
  return out;
}

async function collectTreasury(env: Env): Promise<EconomicEvent[]> {
  const url =
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/" +
    "v2/accounting/od/debt_to_penny?sort=-record_date&page[size]=1";
  const data = await fetchJson(url);
  const row = data?.data?.[0];
  if (!row) return [];

  return [{
    source: "us_treasury",
    metric: "fiscal.us_total_public_debt",
    label: "U.S. total public debt",
    value: Number(row.tot_pub_debt_out_amt),
    unit: "USD",
    observed_at: asIso(`${row.record_date}T00:00:00Z`),
    collected_at: nowIso(),
    dimensions: {
      debt_held_public: Number(row.debt_held_public_amt ?? 0),
      intragovernmental: Number(row.intragov_hold_amt ?? 0)
    }
  }];
}

async function collectFRED(env: Env): Promise<EconomicEvent[]> {
  const url = new URL("https://api.stlouisfed.org/fred/series/observations");
  url.searchParams.set("series_id", "ICSA");
  url.searchParams.set("api_key", env.FRED_API_KEY);
  url.searchParams.set("file_type", "json");
  url.searchParams.set("sort_order", "desc");
  url.searchParams.set("limit", "1");

  const data = await fetchJson(url.toString());
  const row = data?.observations?.[0];
  if (!row || row.value === ".") return [];

  return [{
    source: "fred",
    metric: "labor.us_initial_jobless_claims",
    label: "U.S. initial jobless claims",
    value: Number(row.value),
    unit: "claims",
    observed_at: asIso(`${row.date}T00:00:00Z`),
    collected_at: nowIso(),
    dimensions: { series_id: "ICSA" }
  }];
}

async function collectEIA(env: Env): Promise<EconomicEvent[]> {
  const url = new URL("https://api.eia.gov/v2/electricity/rto/region-data/data/");
  url.searchParams.set("api_key", env.EIA_API_KEY);
  url.searchParams.set("frequency", "hourly");
  url.searchParams.append("data[0]", "value");
  url.searchParams.append("facets[respondent][]", "US48");
  url.searchParams.append("facets[type][]", "D");
  url.searchParams.set("sort[0][column]", "period");
  url.searchParams.set("sort[0][direction]", "desc");
  url.searchParams.set("length", "1");

  const data = await fetchJson(url.toString());
  const row = data?.response?.data?.[0];
  if (!row) return [];

  return [{
    source: "eia",
    metric: "energy.us48_electricity_demand",
    label: "U.S. electricity demand",
    value: Number(row.value),
    unit: row["value-units"] || "MW",
    observed_at: asIso(row.period),
    collected_at: nowIso(),
    dimensions: {
      respondent: row.respondent || "US48",
      type: row.type || "D"
    }
  }];
}

async function collectAndProduce(env: Env, source: string): Promise<{source: string, count: number, events: EconomicEvent[]}> {
  let events: EconomicEvent[];

  switch (source) {
    case "coingecko": events = await collectCoinGecko(env); break;
    case "eia": events = await collectEIA(env); break;
    case "nbp": events = await collectNBP(env); break;
    case "treasury": events = await collectTreasury(env); break;
    case "fred": events = await collectFRED(env); break;
    default: throw new Error(`Unknown source: ${source}`);
  }

  await emitAll(env, events);
  return { source, count: events.length, events };
}

async function collectMany(env: Env, sources: string[]) {
  const results: Array<{source: string, count: number, events?: EconomicEvent[], error?: string}> = [];
  for (const source of sources) {
    try {
      results.push(await collectAndProduce(env, source));
    } catch (e) {
      results.push({
        source,
        count: 0,
        error: e instanceof Error ? e.message : String(e)
      });
    }
  }
  return results;
}

async function scheduledCollect(controller: ScheduledController, env: Env): Promise<void> {
  if (env.COLLECTOR_MODE === "coingecko-only") {
    if (controller.cron === "*/5 * * * *") await collectMany(env, ["coingecko"]);
    return;
  }

  switch (controller.cron) {
    case "*/5 * * * *":
      await collectMany(env, ["coingecko"]);
      break;
    case "7 * * * *":
      await collectMany(env, ["eia"]);
      break;
    case "15 16 * * *":
      await collectMany(env, ["nbp", "treasury"]);
      break;
    case "30 16 * * 4":
      await collectMany(env, ["fred"]);
      break;
  }
}

function extractEvents(body: string): any[] {
  const trimmed = body.trim();
  if (!trimmed) return [];

  try {
    const parsed = JSON.parse(trimmed);
    return Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    // HTTP Sink batching may be configured as newline-delimited JSON.
    return trimmed
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  }
}

async function ingest(request: Request, env: Env): Promise<Response> {
  if (!authOk(request, env)) return new Response("Unauthorized", { status: 401 });

  const events = extractEvents(await request.text());
  const received = nowIso();

  for (const raw of events) {
    const event = (raw?.value !== null && typeof raw?.value === "object") ? raw.value : raw;
    if (!event?.metric) continue;

    await env.DB.prepare(`
      INSERT INTO latest_metrics
        (metric, source, label, value, unit, observed_at, collected_at, dimensions_json, received_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(metric) DO UPDATE SET
        source=excluded.source,
        label=excluded.label,
        value=excluded.value,
        unit=excluded.unit,
        observed_at=excluded.observed_at,
        collected_at=excluded.collected_at,
        dimensions_json=excluded.dimensions_json,
        received_at=excluded.received_at
    `).bind(
      String(event.metric),
      String(event.source ?? "unknown"),
      String(event.label ?? event.metric),
      Number(event.value),
      String(event.unit ?? ""),
      String(event.observed_at ?? received),
      String(event.collected_at ?? received),
      JSON.stringify(event.dimensions ?? {}),
      received
    ).run();
  }

  return new Response(JSON.stringify({ ok: true, received: events.length }), {
    headers: jsonHeaders
  });
}

async function dashboardData(env: Env): Promise<Response> {
  const result = await env.DB.prepare(`
    SELECT metric, source, label, value, unit, observed_at, collected_at,
           dimensions_json, received_at
    FROM latest_metrics
    ORDER BY source, metric
  `).all();

  const rows = (result.results || []).map((r: any) => ({
    ...r,
    dimensions: JSON.parse(r.dimensions_json || "{}"),
    dimensions_json: undefined
  }));

  return new Response(JSON.stringify({
    generated_at: nowIso(),
    metrics: rows
  }), {
    headers: {
      ...jsonHeaders,
      "cache-control": "no-store"
    }
  });
}

function buildPage(token: string): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Economic Pulse</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#111827;background:#f6f7f9}
*{box-sizing:border-box}body{margin:0}
.wrap{max-width:1200px;margin:0 auto;padding:32px 22px 70px}

/* ── page header ── */
header{display:flex;justify-content:space-between;gap:20px;align-items:center;margin-bottom:32px}
h1{font-size:22px;font-weight:700;letter-spacing:-.4px;margin:0;color:#111827}
.sub{color:#6b7280;margin-top:4px;font-size:13px}
.status{font-size:12px;color:#9ca3af;text-align:right}

/* ── scoreboard ── */
.scoreboard{background:white;border:1px solid #e5e7eb;border-radius:20px;padding:28px 28px 20px;margin-bottom:28px;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.sb-title{font-size:11px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:#9ca3af;margin-bottom:20px}
.sb-groups{display:flex;flex-wrap:wrap;gap:0}
.sb-group{flex:1;min-width:200px;padding:0 24px 16px 0;border-right:1px solid #f3f4f6}
.sb-group:last-child{border-right:none;padding-right:0}
.sb-group-name{font-size:10px;font-weight:700;letter-spacing:.13em;text-transform:uppercase;color:#9ca3af;margin-bottom:12px}
.sb-item{margin-bottom:14px}
.sb-label{font-size:11px;color:#6b7280;margin-bottom:3px}
.sb-val{font-size:28px;font-weight:800;letter-spacing:-1px;color:#111827;line-height:1}
.sb-val.stale{color:#d1d5db}
.sb-unit{font-size:12px;font-weight:400;color:#9ca3af;margin-left:4px}
.sb-age{font-size:10px;color:#9ca3af;margin-top:4px}

/* ── collect row ── */
.collect-row{display:flex;align-items:center;gap:14px;margin-bottom:20px}
.btn{display:inline-flex;align-items:center;gap:8px;padding:9px 18px;border:none;border-radius:10px;background:#1d4ed8;color:white;font-size:13px;font-weight:600;cursor:pointer;transition:background .15s}
.btn:hover{background:#1e40af}.btn:disabled{background:#93c5fd;color:white;cursor:not-allowed}
.collect-msg{font-size:13px;color:#6b7280}

/* ── raw collected box ── */
.raw-box{background:white;border:1px solid #e5e7eb;border-radius:16px;padding:20px;margin-bottom:22px}
.raw-box h2{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.12em;color:#9ca3af;margin:0 0 14px}
.raw-table{width:100%;border-collapse:collapse;font-size:13px}
.raw-table th{text-align:left;color:#6b7280;font-weight:600;padding:4px 10px 8px 0;border-bottom:1px solid #f3f4f6}
.raw-table td{padding:6px 10px 6px 0;border-bottom:1px solid #f9fafb;color:#374151}
.raw-table td.val{font-weight:700;color:#111827;text-align:right}
.pipeline-note{font-size:11px;color:#9ca3af;margin-top:12px}

/* ── detail cards ── */
.section-title{font-size:11px;font-weight:700;letter-spacing:.13em;text-transform:uppercase;color:#9ca3af;margin-bottom:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin-bottom:28px}
.card{background:white;border:1px solid #e5e7eb;border-radius:14px;padding:18px;box-shadow:0 1px 2px rgba(0,0,0,.03)}
.src{text-transform:uppercase;font-size:10px;letter-spacing:.12em;color:#9ca3af;font-weight:700}
.lbl{font-size:14px;margin-top:7px;color:#374151}
.val{font-size:28px;font-weight:720;letter-spacing:-.6px;margin-top:10px;color:#111827}
.unt{font-size:12px;color:#6b7280;margin-left:4px}
.time{margin-top:12px;font-size:11px;color:#9ca3af}
.empty{padding:28px;background:white;border:1px solid #e5e7eb;border-radius:14px;color:#6b7280;font-size:13px}

footer{margin-top:16px;color:#9ca3af;font-size:11px}
</style>
</head>
<body>
<div class="wrap">

<header>
  <div>
    <h1>Economic Pulse</h1>
    <div class="sub">Live economic signals · Confluent → Flink → Cloudflare</div>
  </div>
  <div class="status" id="status">Loading…</div>
</header>

<!-- SCOREBOARD -->
<div class="scoreboard">
  <div class="sb-title">Live Metrics</div>
  <div class="sb-groups">

    <div class="sb-group">
      <div class="sb-group-name">Crypto</div>
      <div class="sb-item">
        <div class="sb-label">Bitcoin</div>
        <div class="sb-val stale" id="sb-crypto.bitcoin.usd">—<span class="sb-unit">USD</span></div>
        <div class="sb-age" id="sb-age-crypto.bitcoin.usd"></div>
      </div>
      <div class="sb-item">
        <div class="sb-label">Ethereum</div>
        <div class="sb-val stale" id="sb-crypto.ethereum.usd">—<span class="sb-unit">USD</span></div>
        <div class="sb-age" id="sb-age-crypto.ethereum.usd"></div>
      </div>
    </div>

    <div class="sb-group" style="padding-left:24px">
      <div class="sb-group-name">FX Rates (PLN)</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0 16px">
        <div class="sb-item">
          <div class="sb-label">USD/PLN</div>
          <div class="sb-val stale" id="sb-fx.usd_pln">—</div>
          <div class="sb-age" id="sb-age-fx.usd_pln"></div>
        </div>
        <div class="sb-item">
          <div class="sb-label">EUR/PLN</div>
          <div class="sb-val stale" id="sb-fx.eur_pln">—</div>
          <div class="sb-age" id="sb-age-fx.eur_pln"></div>
        </div>
        <div class="sb-item">
          <div class="sb-label">GBP/PLN</div>
          <div class="sb-val stale" id="sb-fx.gbp_pln">—</div>
          <div class="sb-age" id="sb-age-fx.gbp_pln"></div>
        </div>
        <div class="sb-item">
          <div class="sb-label">CHF/PLN</div>
          <div class="sb-val stale" id="sb-fx.chf_pln">—</div>
          <div class="sb-age" id="sb-age-fx.chf_pln"></div>
        </div>
      </div>
    </div>

    <div class="sb-group" style="padding-left:24px">
      <div class="sb-group-name">Energy</div>
      <div class="sb-item">
        <div class="sb-label">U.S. Electricity Demand</div>
        <div class="sb-val stale" id="sb-energy.us48_electricity_demand">—<span class="sb-unit">MW</span></div>
        <div class="sb-age" id="sb-age-energy.us48_electricity_demand"></div>
      </div>
    </div>

    <div class="sb-group" style="padding-left:24px;border-right:none">
      <div class="sb-group-name">Macro</div>
      <div class="sb-item">
        <div class="sb-label">U.S. Public Debt</div>
        <div class="sb-val stale" id="sb-fiscal.us_total_public_debt">—<span class="sb-unit">USD</span></div>
        <div class="sb-age" id="sb-age-fiscal.us_total_public_debt"></div>
      </div>
      <div class="sb-item">
        <div class="sb-label">Initial Jobless Claims</div>
        <div class="sb-val stale" id="sb-labor.us_initial_jobless_claims">—<span class="sb-unit">k</span></div>
        <div class="sb-age" id="sb-age-labor.us_initial_jobless_claims"></div>
      </div>
    </div>

  </div>
</div>

<!-- COLLECT BUTTON -->
<div class="collect-row">
  <button class="btn" id="collectBtn">⚡ Collect Energy (U.S. Energy Information Administration)</button>
  <span class="collect-msg" id="collectStatus"></span>
</div>

<!-- RAW RESULT BOX -->
<div id="rawBox" class="raw-box" style="display:none">
  <h2>Collected from U.S. Energy Information Administration — sent to Kafka ✓</h2>
  <table class="raw-table">
    <thead><tr><th>Metric</th><th>Label</th><th style="text-align:right">Value</th><th>Unit</th><th>Observed</th></tr></thead>
    <tbody id="rawBody"></tbody>
  </table>
  <div class="pipeline-note">↓ Flink normalizes → HTTP Sink → /api/ingest → D1 → scoreboard &amp; cards update automatically</div>
</div>

<!-- DETAIL CARDS -->
<div class="section-title">All metrics detail</div>
<div id="grid" class="grid"></div>

<footer>CoinGecko · U.S. Energy Information Administration · National Bank of Poland · U.S. Treasury · FRED → Confluent → Flink → Cloudflare</footer>
</div>
<script>
const TOKEN = ${JSON.stringify(token)};

const SOURCE_NAMES = {
  coingecko: "CoinGecko",
  nbp: "National Bank of Poland",
  us_treasury: "U.S. Treasury",
  fred: "FRED / St. Louis Fed",
  eia: "U.S. Energy Information Administration"
};

function sourceName(s){ return SOURCE_NAMES[s] || s; }

function fmt(x, unit){
  if (!Number.isFinite(x)) return "—";
  if(unit==="USD" && x>1e12) return "$"+(x/1e12).toFixed(2)+"T";
  if(unit==="USD" && x>1e9)  return "$"+(x/1e9).toFixed(2)+"B";
  if(unit==="USD") return "$"+x.toLocaleString(undefined,{maximumFractionDigits:2});
  if(unit==="PLN") return x.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:4});
  if(unit==="MW")  return x.toLocaleString(undefined,{maximumFractionDigits:0});
  if(unit==="claims") return (x/1000).toLocaleString(undefined,{minimumFractionDigits:1,maximumFractionDigits:1});
  return x.toLocaleString(undefined,{maximumFractionDigits:2});
}

function fmtSb(x, unit){
  if (!Number.isFinite(x)) return "—";
  if(unit==="USD" && x>1e12) return (x/1e12).toFixed(2)+"<span class='sb-unit'>T USD</span>";
  if(unit==="USD" && x>1e9)  return (x/1e9).toFixed(2)+"<span class='sb-unit'>B USD</span>";
  if(unit==="USD") return "$"+x.toLocaleString(undefined,{maximumFractionDigits:2});
  if(unit==="PLN") return x.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:4});
  if(unit==="MW")  return x.toLocaleString(undefined,{maximumFractionDigits:0})+"<span class='sb-unit'>MW</span>";
  if(unit==="claims") return (x/1000).toLocaleString(undefined,{minimumFractionDigits:1,maximumFractionDigits:1})+"<span class='sb-unit'>k</span>";
  return x.toLocaleString(undefined,{maximumFractionDigits:2});
}

function ago(iso){
  if(!iso) return "";
  const d=new Date(iso), s=Math.max(0,(Date.now()-d.getTime())/1000);
  if(s<90)     return Math.round(s)+" sec ago";
  if(s<5400)   return Math.round(s/60)+" min ago";
  if(s<129600) return Math.round(s/3600)+" h ago";
  return Math.round(s/86400)+" d ago";
}

function updateScoreboard(metrics){
  for(const m of metrics){
    const el=document.getElementById("sb-"+m.metric);
    const ageEl=document.getElementById("sb-age-"+m.metric);
    if(!el) continue;
    const x=Number(m.value);
    el.innerHTML=fmtSb(x,m.unit);
    el.classList.remove("stale");
    if(ageEl) ageEl.textContent=ago(m.observed_at);
  }
}

async function load(){
  try{
    const r=await fetch("/api/dashboard",{cache:"no-store"});
    const data=await r.json();
    const grid=document.getElementById("grid");
    updateScoreboard(data.metrics);
    if(!data.metrics.length){
      grid.innerHTML='<div class="empty">No metrics yet — press the button above to collect, then wait for the Confluent pipeline to deliver them back.</div>';
    } else {
      grid.innerHTML=data.metrics.map(m=>\`
        <article class="card">
          <div class="src">\${sourceName(m.source)}</div>
          <div class="lbl">\${m.label}</div>
          <div class="val">\${fmt(Number(m.value),m.unit)}<span class="unt">\${m.unit==="USD"||m.unit==="PLN"?"":m.unit}</span></div>
          <div class="time">Observed \${ago(m.observed_at)} · received \${ago(m.received_at)}</div>
        </article>\`).join("");
    }
    document.getElementById("status").textContent="Updated "+new Date(data.generated_at).toLocaleTimeString();
  }catch(e){
    document.getElementById("status").textContent="Dashboard unavailable";
  }
}

async function collectEIA(){
  const btn=document.getElementById("collectBtn");
  const lbl=document.getElementById("collectStatus");
  const rawBox=document.getElementById("rawBox");
  const rawBody=document.getElementById("rawBody");
  btn.disabled=true;
  lbl.textContent="Collecting…";
  rawBox.style.display="none";
  try{
    const r=await fetch("/api/collect?source=eia",{headers:{"Authorization":"Bearer "+TOKEN}});
    const d=await r.json();
    const res=d.results&&d.results[0];
    if(res&&!res.error&&res.events&&res.events.length){
      lbl.textContent="✓ "+res.count+" event(s) sent to Kafka";
      rawBody.innerHTML=res.events.map(e=>\`<tr>
        <td style="font-family:monospace;font-size:11px;color:#6b7280">\${e.metric}</td>
        <td>\${e.label}</td>
        <td class="val">\${Number(e.value).toLocaleString(undefined,{maximumFractionDigits:2})}</td>
        <td>\${e.unit}</td>
        <td>\${ago(e.observed_at)}</td>
      </tr>\`).join("");
      rawBox.style.display="block";
    } else {
      lbl.textContent="✗ "+(res&&res.error||"no events returned");
    }
    setTimeout(load,4000);
  }catch(e){
    lbl.textContent="✗ request failed";
  }finally{
    btn.disabled=false;
  }
}

document.getElementById("collectBtn").addEventListener("click",collectEIA);
load(); setInterval(load,30000);
</script>
</body>
</html>`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return new Response(buildPage(env.DASHBOARD_INGEST_TOKEN), { headers: { "content-type": "text/html; charset=utf-8" } });
    }

    if (request.method === "GET" && url.pathname === "/api/dashboard") {
      return dashboardData(env);
    }

    if (request.method === "POST" && url.pathname === "/api/ingest") {
      return ingest(request, env);
    }

    if (request.method === "GET" && url.pathname === "/api/collect") {
      if (!authOk(request, env)) return new Response("Unauthorized", { status: 401 });
      const source = url.searchParams.get("source") || "all";
      const sources = source === "all"
        ? ["coingecko", "eia", "nbp", "treasury", "fred"]
        : [source];
      const results = await collectMany(env, sources);
      return new Response(JSON.stringify({ ok: true, results }, null, 2), { headers: jsonHeaders });
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(scheduledCollect(controller, env));
  }
};
