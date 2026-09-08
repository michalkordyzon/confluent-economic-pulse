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

async function collectAndProduce(env: Env, source: string): Promise<{source: string, count: number}> {
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
  return { source, count: events.length };
}

async function collectMany(env: Env, sources: string[]) {
  const results: Array<{source: string, count: number, error?: string}> = [];
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
    const event = raw?.value ?? raw;
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

const page = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Economic Pulse</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#111827;background:#f6f7f9}
*{box-sizing:border-box}body{margin:0}.wrap{max-width:1100px;margin:0 auto;padding:40px 22px 70px}
header{display:flex;justify-content:space-between;gap:20px;align-items:end;margin-bottom:26px}
h1{font-size:36px;letter-spacing:-1.3px;margin:0}.sub{color:#6b7280;margin-top:8px}
.status{font-size:13px;color:#6b7280;text-align:right}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px}
.card{background:white;border:1px solid #e5e7eb;border-radius:16px;padding:20px;box-shadow:0 1px 2px rgba(0,0,0,.03)}
.source{text-transform:uppercase;font-size:11px;letter-spacing:.12em;color:#6b7280;font-weight:700}
.label{font-size:15px;margin-top:8px;color:#374151}.value{font-size:30px;font-weight:720;letter-spacing:-.7px;margin-top:12px}
.unit{font-size:13px;color:#6b7280;margin-left:5px}.time{margin-top:14px;font-size:12px;color:#9ca3af}
footer{margin-top:28px;color:#9ca3af;font-size:12px}
.empty{padding:30px;background:white;border:1px solid #e5e7eb;border-radius:16px;color:#6b7280}
</style>
</head>
<body>
<div class="wrap">
<header>
  <div><h1>Economic Pulse</h1><div class="sub">Live economic signals flowing through Confluent.</div></div>
  <div class="status" id="status">Loading…</div>
</header>
<div id="grid" class="grid"></div>
<footer>CoinGecko · U.S. EIA · NBP · U.S. Treasury · FRED → Confluent → Flink → Cloudflare</footer>
</div>
<script>
function fmt(x, unit){
  if (!Number.isFinite(x)) return "—";
  if(unit==="USD" && x>1e12) return "$"+(x/1e12).toFixed(2)+"T";
  if(unit==="USD" && x>1e9) return "$"+(x/1e9).toFixed(2)+"B";
  if(unit==="USD") return "$"+x.toLocaleString(undefined,{maximumFractionDigits:2});
  if(unit==="PLN") return x.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:4});
  if(unit==="MW") return x.toLocaleString(undefined,{maximumFractionDigits:0});
  if(unit==="claims") return x.toLocaleString(undefined,{maximumFractionDigits:0});
  return x.toLocaleString(undefined,{maximumFractionDigits:2});
}
function ago(iso){
  const d=new Date(iso), s=Math.max(0,(Date.now()-d.getTime())/1000);
  if(s<90) return Math.round(s)+" sec ago";
  if(s<5400) return Math.round(s/60)+" min ago";
  if(s<129600) return Math.round(s/3600)+" h ago";
  return Math.round(s/86400)+" d ago";
}
async function load(){
  try{
    const r=await fetch("/api/dashboard",{cache:"no-store"});
    const data=await r.json();
    const grid=document.getElementById("grid");
    if(!data.metrics.length){
      grid.innerHTML='<div class="empty">No metrics yet. Trigger a collector, then let Confluent send normalized events back through the HTTP Sink.</div>';
    } else {
      grid.innerHTML=data.metrics.map(m=>\`
        <article class="card">
          <div class="source">\${m.source.replaceAll("_"," ")}</div>
          <div class="label">\${m.label}</div>
          <div class="value">\${fmt(Number(m.value),m.unit)}<span class="unit">\${m.unit==="USD"?"":m.unit}</span></div>
          <div class="time">Observed \${ago(m.observed_at)} · received \${ago(m.received_at)}</div>
        </article>\`).join("");
    }
    document.getElementById("status").textContent="Updated "+new Date(data.generated_at).toLocaleTimeString();
  }catch(e){
    document.getElementById("status").textContent="Dashboard unavailable";
  }
}
load(); setInterval(load,30000);
</script>
</body>
</html>`;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return new Response(page, { headers: { "content-type": "text/html; charset=utf-8" } });
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
