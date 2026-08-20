import os
import sys
import re
import json
import time
import socket
import threading
import http.server
import urllib.request
from urllib.parse import urlparse, parse_qs
import psycopg2

# Ungepuffertes Logging für Render erzwingen
sys.stdout.reconfigure(line_buffering=True)
print("=== APRS SERVER STARTET (FM-FUNKNETZ TG 313 + VEHICLE FILTER) ===", flush=True)

# ==========================================
# KONFIGURATION
# ==========================================
CALLSIGN = "DL4KAL"
PASSCODE = "17065"

# Unbeschränkter APRS-IS Empfang (keine Radius-Einschränkung)
FILTER_RULE = "" 
DATABASE_URL = os.environ.get("DATABASE_URL")

# Erlaubte APRS-Symbole: Wohnmobile (v, R), Segelyachten ($, Y), Motorboote (b)
ALLOWED_SYMBOLS = {'/v', '\\v', '/$', '\\$', '/b', '\\b', '/R', '\\R', '/Y', '\\Y'}

ACTIVE_STATIONS = {}
STORAGE_LOCK = threading.Lock()

# Globale Liste für aktive Rufzeichen im FM-Funknetz auf TG 313
TG313_ACTIVE_CALLSIGNS = set()

def update_tg313_stations():
    """Holt alle 15 Sekunden die aktiven Nodes der TG 313 aus dem FM-Funknetz"""
    global TG313_ACTIVE_CALLSIGNS
    urls = [
        "https://livemap.fm-funknetz.de/api/status",
        "https://fm-funknetz.de/api/status.json"
    ]
    
    while True:
        success = False
        for url in urls:
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
                with urllib.request.urlopen(req, timeout=10) as response:
                    if response.status == 200:
                        raw_data = response.read().decode('utf-8')
                        data = json.loads(raw_data)
                        active = set()
                        
                        nodes = data.get('nodes', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                        for node in nodes:
                            tg_single = str(node.get('tg', node.get('active_tg', node.get('default_tg', ''))))
                            tg_list = [str(x) for x in node.get('active_tgs', [])]
                            
                            if '313' in tg_list or tg_single == '313':
                                call = node.get('callsign', node.get('name', node.get('sysop', '')))
                                if call:
                                    base_call = call.split('-')[0].strip().upper()
                                    active.add(base_call)
                        
                        with STORAGE_LOCK:
                            TG313_ACTIVE_CALLSIGNS = active
                        print(f"[FM-FUNKNETZ] Aktive TG313 Rufzeichen: {list(TG313_ACTIVE_CALLSIGNS)}", flush=True)
                        success = True
                        break
            except Exception:
                continue
        
        if not success:
            print("[FM-FUNKNETZ] Status-API nicht erreichbar, versuche erneut...", flush=True)
            
        time.sleep(15)

# Starte den FM-Funknetz-Updater als Hintergrund-Thread
fm_thread = threading.Thread(target=update_tg313_stations, daemon=True)
fm_thread.start()

def parse_aprs(raw):
    try:
        match = re.search(r'(?P<lat>\d{4}\.\d{2}[NS])\s*(?P<table_id>[/\\])(?P<lon>\d{5}\.\d{2}[EW])(?P<sym>.)', raw)
        if match:
            d = match.groupdict()
            lat = int(d['lat'][:2]) + float(d['lat'][2:7]) / 60
            if 'S' in d['lat']: 
                lat = -lat
            lon = int(d['lon'][:3]) + float(d['lon'][3:8]) / 60
            if 'W' in d['lon']: 
                lon = -lon
            
            symbol_code = d['table_id'] + d['sym']
            
            # SYMBOL-FILTER: Nur Wohnmobile, Segelyachten & Motorboote
            if symbol_code not in ALLOWED_SYMBOLS:
                return None
            
            return {
                "lat": round(lat, 5),
                "lon": round(lon, 5),
                "sym": symbol_code,
                "comment": raw.strip()
            }
    except Exception:
        return None
    return None

def save_to_db(callsign, lat, lon, symbol, comment):
    """Speichert jede empfangene TG313-Bake dauerhaft in Supabase"""
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO aprs_history (callsign, latitude, longitude, symbol, comment)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (callsign, lat, lon, symbol, str(comment)[:100])
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB-WRITE-ERROR] {e}", flush=True)

def get_history_from_db(callsign=None, hours=24):
    """Liest historische Punkte der letzten X Stunden aus Supabase"""
    if not DATABASE_URL:
        return []
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        cur = conn.cursor()
        
        if callsign:
            cur.execute(
                """
                SELECT callsign, latitude, longitude, symbol, timestamp 
                FROM aprs_history 
                WHERE callsign ILIKE %s AND timestamp >= NOW() - (%s || ' hours')::INTERVAL
                ORDER BY timestamp ASC
                """,
                (f"%{callsign}%", str(hours))
            )
        else:
            cur.execute(
                """
                SELECT callsign, latitude, longitude, symbol, timestamp 
                FROM aprs_history 
                WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
                ORDER BY timestamp ASC
                """,
                (str(hours),)
            )
            
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"callsign": r[0], "lat": r[1], "lon": r[2], "symbol": r[3], "time": r[4].isoformat()} for r in rows]
    except Exception as e:
        print(f"[DB-READ-ERROR] {e}", flush=True)
        return []

def aprs_stream_worker():
    host = 'euro.aprs2.net'
    port = 14580
    userline = f'user {CALLSIGN} pass {PASSCODE} vers FM-Funknetz-Server 1.4\r\n'
    
    while True:
        try:
            print(f"[APRS] Verbinde zu {host}:{port}...", flush=True)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(30)
            s.connect((host, port))
            s.sendall(userline.encode('utf-8'))
            print("[APRS] Handshake gesendet.", flush=True)
            
            buffer = ""
            while True:
                data = s.recv(4096).decode('utf-8', errors='ignore')
                if not data:
                    break
                buffer += data
                lines = buffer.split('\r\n')
                buffer = lines.pop()
                
                for line in lines:
                    if not line or line.startswith('#'):
                        continue
                    
                    parsed = parse_aprs(line)
                    if parsed and '>' in line:
                        station_call = line.split('>')[0].strip()
                        base_call = station_call.split('-')[0].strip().upper()
                        
                        # Pruefe TG313 Aktivitaet aus FM-Funknetz
                        with STORAGE_LOCK:
                            is_active_on_tg = base_call in TG313_ACTIVE_CALLSIGNS
                        
                        if is_active_on_tg:
                            print(f"[TREFFER TG313 + SYMBOL] {station_call} ({parsed['sym']}) -> {parsed['lat']}, {parsed['lon']}", flush=True)
                            
                            with STORAGE_LOCK:
                                ACTIVE_STATIONS[station_call] = {
                                    "callsign": station_call,
                                    **parsed
                                }
                            
                            threading.Thread(
                                target=save_to_db,
                                args=(station_call, parsed["lat"], parsed["lon"], parsed["sym"], parsed["comment"]),
                                daemon=True
                            ).start()

        except Exception as e:
            print(f"[APRS-ERROR] {e}. Reconnect in 10s...", flush=True)
            time.sleep(10)

aprs_thread = threading.Thread(target=aprs_stream_worker, daemon=True)
aprs_thread.start()

class APRSRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed_url = urlparse(self.path)
        
        if parsed_url.path == '/data.json':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self._send_cors_headers()
            self.end_headers()
            
            with STORAGE_LOCK:
                payload = json.dumps(list(ACTIVE_STATIONS.values()))
            self.wfile.write(payload.encode('utf-8'))

        elif parsed_url.path == '/history.json':
            query_params = parse_qs(parsed_url.query)
            callsign = query_params.get('callsign', [None])[0]
            hours = int(query_params.get('hours', [24])[0])
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self._send_cors_headers()
            self.end_headers()
            
            history_data = get_history_from_db(callsign=callsign, hours=hours)
            self.wfile.write(json.dumps(history_data).encode('utf-8'))

        elif parsed_url.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(b"APRS Server online! (FM-Funknetz TG 313 + Fahrzeug-Filter)")

        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()

def run_server():
    port = int(os.environ.get("PORT", 10000))
    print(f"[SERVER] Binde Server an Port {port}...", flush=True)
    httpd = http.server.HTTPServer(('', port), APRSRequestHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()
