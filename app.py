import socket
import threading
import http.server
import json
import time
import re
import os
from urllib.parse import urlparse, parse_qs

# ==========================================
# EINSTELLUNGEN
# ==========================================
CALLSIGN = "DL4KAL"
PASSCODE = "17065"

# Filter für Wohnmobile (R), Segelyachten (Y) und Motorboote (s)
FILTER_RULE = "s/R s/Y s/s"
# ==========================================

ACTIVE_STATIONS = {}
STORAGE_LOCK = threading.Lock()

def parse_aprs(raw):
    """
    Parst APRS-Pakete und extrahiert Koordinaten sowie Symbole.
    """
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
            
            return {
                "lat": round(lat, 5),
                "lon": round(lon, 5),
                "sym": d['table_id'] + d['sym'],
                "course": "0",
                "speed": "0",
                "alt": 0,
                "comment": raw.strip()
            }
    except Exception:
        return None
    return None

def aprs_stream_worker():
    """
    Hält die dauerhafte TCP-Socket-Verbindung zum APRS-IS Server aufrecht.
    """
    host = 'euro.aprs2.net'
    port = 14580
    userline = f'user {CALLSIGN} pass {PASSCODE} vers PythonCloudServer 3.7 filter {FILTER_RULE}\r\n'
    
    while True:
        try:
            print(f"[APRS] Verbinde zu {host}:{port}...", flush=True)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(30)
            s.connect((host, port))
            s.sendall(userline.encode('utf-8'))
            print("[APRS] Handshake & Filter gesendet.", flush=True)
            
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
                        with STORAGE_LOCK:
                            ACTIVE_STATIONS[station_call] = {
                                "callsign": station_call,
                                **parsed
                            }
                        print(f"[APRS-Empfang] {station_call} @ {parsed['lat']}, {parsed['lon']}", flush=True)

        except Exception as e:
            print(f"[APRS-ERROR] {e}. Reconnect in 10s...", flush=True)
            time.sleep(10)

# Thread sofort beim Laden des Moduls starten
aprs_thread = threading.Thread(target=aprs_stream_worker, daemon=True)
aprs_thread.start()

# ==========================================
# HTTP REQUEST HANDLER (API & RENDER HEALTH CHECK)
# ==========================================
class APRSRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Unterdrückt uninteressanten HTTP-Access-Log Spamm

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

        elif parsed_url.path == '/':
            # WICHTIG FÜR RENDER: Beantwortet den Health-Check
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(b"<h3>APRS Cloud Server online!</h3><p>Nutze /data.json fuer die Daten.</p>")

        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(b"Not Found")

def run_server():
    port = int(os.environ.get("PORT", 10000))
    server_address = ('', port)
    httpd = http.server.HTTPServer(server_address, APRSRequestHandler)
    print(f"[HTTP] Server laeuft auf Port {port}...", flush=True)
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()
