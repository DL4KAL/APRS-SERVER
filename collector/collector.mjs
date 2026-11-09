import net from "node:net";

const HOST = "euro.aprs2.net";
const PORT = 10152;
const CALL = process.env.APRS_CALL;
const PASS = process.env.APRS_PASS;
const FILTER = "r/51.1643/10.4542/2500";
const LISTEN_MS = 270_000;
const MAX_KM = 2500;
const REF_LAT = 51.1643;
const REF_LON = 10.4542;

const WANTED = new Set(["/R", "/Y", "/s"]);
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_KEY;
const RETENTION_H = 24;

const rows = new Map(); // Rufzeichen -> letzte Position dieses Laufs

function distKm(lat1, lon1, lat2, lon2) {
  const R = 6371, rad = (d) => (d * Math.PI) / 180;
  const dLat = rad(lat2 - lat1), dLon = rad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(rad(lat1)) * Math.cos(rad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function parse(line) {
  const idx = line.indexOf(":");
  if (idx < 0 || !line.includes(">")) return null;
  const call = line.slice(0, line.indexOf(">")).trim();
  const info = line.slice(idx + 1);

  const m = info.match(/(\d{4}\.\d{2})([NS])(.)(\d{5}\.\d{2})([EW])(.)/);
  if (!m) return null;

  const symbol = "/" + m[6];
  if (!WANTED.has(m[3] + m[6]) && !WANTED.has(symbol)) return null;

  let lat = parseInt(m[1].slice(0, 2)) + parseFloat(m[1].slice(2)) / 60;
  let lon = parseInt(m[4].slice(0, 3)) + parseFloat(m[4].slice(3)) / 60;
  if (m[2] === "S") lat = -lat;
  if (m[5] === "W") lon = -lon;

  if (distKm(REF_LAT, REF_LON, lat, lon) > MAX_KM) return null;

  return {
    callsign: call,
    latitude: Math.round(lat * 1e5) / 1e5,
    longitude: Math.round(lon * 1e5) / 1e5,
    symbol: symbol,
    comment: line.slice(idx + 1, idx + 101),
    timestamp: new Date().toISOString(),
  };
}

async function flush() {
  const data = [...rows.values()];
  console.log(`Schreibe ${data.length} Positionen`);

  if (data.length) {
    const res = await fetch(`${SUPA_URL}/rest/v1/aprs_history`, {
      method: "POST",
      headers: {
        apikey: SUPA_KEY,
        Authorization: `Bearer ${SUPA_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify(data),
    });
    if (!res.ok) throw new Error(`Supabase ${res.status}: ${await res.text()}`);
  }

  // NEU: Einträge älter als 24 Stunden entfernen
  const cutoff = new Date(Date.now() - RETENTION_H * 3600 * 1000).toISOString();
  const del = await fetch(
    `${SUPA_URL}/rest/v1/aprs_history?timestamp=lt.${encodeURIComponent(cutoff)}`,
    {
      method: "DELETE",
      headers: {
        apikey: SUPA_KEY,
        Authorization: `Bearer ${SUPA_KEY}`,
        Prefer: "return=minimal",
      },
    }
  );
  if (!del.ok) console.error("Cleanup fehlgeschlagen:", del.status);
  else console.log(`Einträge älter als ${RETENTION_H} h entfernt`);
}

const sock = net.createConnection({ host: HOST, port: PORT });
sock.setEncoding("utf8");
let buf = "";

sock.on("connect", () => {
  sock.write(`user ${CALL} pass ${PASS} vers gh-collector 1.0 filter ${FILTER}\r\n`);
});

sock.on("data", (chunk) => {
  buf += chunk;
  const lines = buf.split("\r\n");
  buf = lines.pop();
  for (const line of lines) {
    if (!line || line.startsWith("#")) continue;
    const row = parse(line);
    if (row) rows.set(row.callsign, row);
  }
});

sock.on("error", (e) => console.error("Socket-Fehler:", e.message));

setTimeout(() => sock.end(), LISTEN_MS);
sock.on("close", async () => {
  try {
    await flush();
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
});
