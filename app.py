import socket
import re
import threading
import json
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import psycopg2

# ==========================================
# KONFIGURATION
# ==========================================
APRS_SERVER = "euro.aprs2.net"
APRS_PORT = 14580
CALLSIGN = "DL4KAL"
PASSCODE = "17065"  # Hier deinen echten Passcode beibehalten

# Filter: 500 km Umkreis um Bonn
APRS_FILTER = "filter r/50.7/7.1/500\r\n"

DATABASE_URL = os.environ.get("DATABASE_URL")

latest_stations = {}
lock = threading.Lock()

# Regex für Standard-APRS-Positionsbaken (!, =, @, /)
APRS_REGEX = re.compile(
    r'([A-Z0-9\-]+)>.*?:[@=!/]([0-9]{4}\.[0-9]{2}[NS])([/\\].)([0-9]{5}\.[0-9]{2}[EW])(.)(.*)'
)

def parse_coord(coord_str, is_lat=True):
    try:
        if is_lat:
            deg = float(coord_str[:2])
            minutes = float(coord_str[2:-1])
            val = deg + (minutes / 60.0)
            return -val if coord_str[-1] == 'S' else val
        else:
            deg = float(coord_str[:3])
            minutes = float(coord_str[3:-1])
            val = deg + (minutes / 60.0)
            return -val if coord_str[-1] == 'W' else val
    except Exception:
        return None

def save_to_supabase(callsign, lat, lon, symbol, comment):
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO aprs_history (callsign, latitude, longitude, symbol, comment)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (callsign, lat, lon, symbol, comment[:100])
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB-ERROR] {e}")

def get_history_from_supabase(callsign, hours=24):
    if not DATABASE_URL:
        return []
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT latitude, longitude, timestamp, symbol
            FROM aprs_history
            WHERE callsign = %s AND timestamp >= NOW() - INTERVAL '%s hours'
            ORDER BY timestamp ASC
            """,
            (callsign, hours)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"lat": r[0], "lon": r[1], "time": r[2].isoformat(), "symbol": r[3]} for r in rows]
    except Exception as e:
        print(f"[DB-ERROR] {e}")
        return []

def aprs_client_thread():
    while True:
        try:
            print(f"[APRS] Verbinde zu {APRS_SERVER}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(30)
            s.connect((APRS_SERVER, APRS_PORT))
            
            auth = f"user {CALLSIGN} pass {PASSCODE} vers DL4KAL-Portal 1.2 {APRS_FILTER}"
            s.sendall(auth.encode('utf-8'))
            
            buffer = ""
            while True:
                data = s.recv(4096).decode('utf-8', errors='ignore')
                if not data:
                    break
                
                buffer += data
                lines = buffer.split('\r\n')
                buffer = lines.pop()
                
                for line in lines:
                    if line.startswith('#') or not line.strip():
                        continue
                    
                    match = APRS_REGEX.search(line)
                    if match:
                        cs = match.group(1)
                        lat = parse_coord(match.group(2), is_lat=True)
                        sym_table = match.group(3)
                        lon = parse_coord(match.group(4), is_lat=False)
                        sym_code = match.group(5)
                        comment = match.group(6).strip()
                        
                        if lat is not None and lon is not None:
                            symbol = f"{sym_table}{sym_code}"
                            
                            with lock:
                                latest_stations[cs] = {
                                    "callsign": cs,
                                    "lat": lat,
                                    "lon": lon,
                                    "symbol": symbol,
                                    "comment": comment
                                }
                            
                            threading.Thread(
                                target=save_to_supabase,
                                args=(cs, lat, lon, symbol, comment),
                                daemon=True
                            ).start()

        except Exception as e:
            print(f"[APRS-ERROR] {e}. Reconnect in 5s...")
            time.sleep(5)

class APRSRequestHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/data.json':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            
            with lock:
                data = list(latest_stations.values())
            self.wfile.write(json.dumps(data).encode('utf-8'))
            
        elif parsed_path.path == '/history.json':
            query_params = parse_qs(parsed_path.query)
            callsign = query_params.get('callsign', [None])[0]
            hours = query_params.get('hours', ['24'])[0]
            
            try:
                hours_int = int(hours)
            except ValueError:
                hours_int = 24

            if callsign:
                history_data = get_history_from_supabase(callsign, hours=hours_int)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps(history_data).encode('utf-8'))
            else:
                self.send_response(400)
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Parameter 'callsign' fehlt"}).encode('utf-8'))
        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(b"Not Found")

def run_http_server():
    port = int(os.environ.get("PORT", 10000))
    server_address = ('', port)
    httpd = HTTPServer(server_address, APRSRequestHandler)
    print(f"[HTTP] Server laeuft auf Port {port}...")
    httpd.serve_forever()

if __name__ == '__main__':
    aprs_thread = threading.Thread(target=aprs_client_thread, daemon=True)
    aprs_thread.start()
    run_http_server()
