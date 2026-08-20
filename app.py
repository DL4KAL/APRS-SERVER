import socket
import threading
import http.server
import json
import time
import re
import os
from urllib.parse import urlparse, parse_qs

# ==========================================
# KONFIGURATION
# ==========================================
CALLSIGN = "DL4KAL"
PASSCODE = "17065"
FILTER_RULE = "s/R s/Y s/s"

ACTIVE_STATIONS = {}
STATION_HISTORY = {}  # Speichert Koordinate + Zeitstempel pro Call
STORAGE_LOCK = threading.Lock()

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
            
            return {
                "lat": round(lat, 5),
                "lon": round(lon, 5),
                "sym": d['table_id'] + d['sym'],
                "comment": raw.strip()
            }
    except Exception:
        return None
    return None

def aprs_stream_worker():
    host = 'euro.aprs2.net'
    port = 14580
    userline = f'user {CALLSIGN} pass {PASSCODE} vers PythonCloudServer 3.9 filter {FILTER_RULE}\r\n'
    
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
                        now_ts = time.time()
                        
                        with STORAGE_LOCK:
                            ACTIVE_STATIONS[station_call] = {
                                "callsign": station_call,
                                **parsed,
                                "timestamp": now_ts
                            }
                            
                            if station_call not in STATION_HISTORY:
                                STATION_HISTORY[station_call] = []
                            
                            history = STATION_HISTORY[station_call]
                            new_entry = {"lat": parsed["lat"], "lon": parsed["lon"], "time": now_ts, "symbol": parsed["sym"]}
                            
                            # Nur anhaengen, wenn sich Position unterscheidet
                            if not history or (history[-1]["lat"] != parsed["lat"] or history[-1]["lon"] != parsed["lon"]):
                                history.append(new_entry)
                                
                            # Bereinige alte Eintraege (älter als 24 Stunden = 86400 Sekunden)
                            STATION_HISTORY[station_call] = [p for p in history if now_ts - p["time"] <= 86400]

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
            
            now_ts = time.time()
            max_age_seconds = hours * 3600
            
            with STORAGE_LOCK:
                if callsign and callsign in STATION_HISTORY:
                    # Filtert exakt nach den angeforderten Stunden
                    filtered = [p for p in STATION_HISTORY[callsign] if now_ts - p["time"] <= max_age_seconds]
                    payload = json.dumps(filtered)
                else:
                    payload = json.dumps([])
            self.wfile.write(payload.encode('utf-8'))

        elif parsed_url.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(b"APRS Server online!")

        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()

def run_server():
    port = int(os.environ.get("PORT", 10000))
    httpd = http.server.HTTPServer(('', port), APRSRequestHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()
