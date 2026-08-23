import os
import sys
import re
import json
import time
import socket
import threading
import http.server
import psycopg2
import math
from urllib.parse import urlparse, parse_qs

# Ungepuffertes Logging für Render erzwingen
sys.stdout.reconfigure(line_buffering=True)
print("=== APRS SERVER STARTET (STABILISIERTER STREAM + FILTER) ===", flush=True)

# ==========================================
# KONFIGURATION
# ==========================================
CALLSIGN = "DL4KAL"
PASSCODE = "17065"
DATABASE_URL = os.environ.get("DATABASE_URL")

# Referenzpunkt für den 2500 km Umkreis (Mitte Deutschland)
REF_LAT = 51.1643
REF_LON = 10.4542
MAX_DISTANCE_KM = 2500.0

ACTIVE_STATIONS = {}
STORAGE_LOCK = threading.Lock()

def calculate_distance(lat1, lon1, lat2, lon2):
    """Berechnet die Entfernung zwischen zwei Punkten in Kilometern (Haversine-Formel)"""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 + 
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
         math.sin(dlon / 2) ** 2)
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def parse_aprs(raw):
    try:
        # Filter auf Symbole: /R, /Y, /s
        match = re.search(r'(?P<lat>\d{4}\.\d{2}[NS])(?P<table_id>/)(?P<lon>\d{5}\.\d{2}[EW])(?P<sym>[RYs])', raw)
        
        if match:
            d = match.groupdict()
            lat = int(d['lat'][:2]) + float(d['lat'][2:7]) / 60
            if 'S' in d['lat']: 
                lat = -lat
            lon = int(d['lon'][:3]) + float(d['lon'][3:8]) / 60
            if 'W' in d['lon']: 
                lon = -lon
            
            # Umkreis-Filter (max 2500 km)
            distance = calculate_distance(REF_LAT, REF_LON, lat, lon)
            if distance > MAX_DISTANCE_KM:
                return None
            
            symbol_code = d['table_id'] + d['sym']

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
    """Speichert jede empfangene Bake in Supabase"""
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

def get_history_from_db(callsign=None, hours=1):
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
    port = 10152
    userline = f'user {CALLSIGN} pass {PASSCODE} vers PythonAPRS 1.0\r\n'
    
    while True:
        s = None
        try:
            print(f"[APRS] Verbinde zu {host}:{port}...", flush=True)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(120)
            s.connect((host, port))
            s.sendall(userline.encode('utf-8'))
            print("[APRS] Login gesendet. Lausche auf Stream...", flush=True)
            
            buffer = ""
            while True:
                data = s.recv(4096).decode('utf-8', errors='ignore')
                if not data:
                    print("[APRS] Server hat die Verbindung geschlossen.", flush=True)
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
                        
                        print(f"[TREFFER] {station_call} ({parsed['sym']}) -> {parsed['lat']}, {parsed['lon']}", flush=True)
                        
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
            print(f"[APRS-ERROR] {e}. Reconnect in 5s...", flush=True)
            time.sleep(5)
        finally:
            if s:
                try:
                    s.close()
                except:
                    pass

aprs_thread = threading.Thread(target=aprs_stream_worker, daemon=True)
aprs_thread.start()

class APRSRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        try:
            parsed_url = urlparse(self.path)
            
            # Hinweis: /data.json wurde entfernt (Livedarstellung entfällt, stattdessen läuft alles über die DB-Historie)
            
            if parsed_url.path == '/history.json':
                query_params = parse_qs(parsed_url.query)
                callsign = query_params.get('callsign', [None])[0]
                
                # Modus-Auswertung: 'live' (entspricht der ehemaligen 1-Stunden-Historie) oder 'long' (48 Stunden)
                mode = query_params.get('mode', ['live'])[0]
                if mode == 'long':
                    hours = 48
                else:
                    hours = 1  # Livedarstellung (letzte Stunde)
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                
                history_data = get_history_from_db(callsign=callsign, hours=hours)
                self.wfile.write(json.dumps(history_data).encode('utf-8'))

            elif parsed_url.path == '/':
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(b"APRS Server online! (Livedarstellung & Historie Lang)")

            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            print(f"[HTTP-ERROR] {e}", flush=True)
            try:
                self.send_response(500)
                self.end_headers()
            except:
                pass

def run_server():
    port = int(os.environ.get("PORT", 10000))
    print(f"Cloud-Server gestartet auf Port {port}", flush=True)
    httpd = http.server.HTTPServer(('', port), APRSRequestHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()
