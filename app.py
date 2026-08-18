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
    # Flexiblerer Regex, der APRS-Pakete unabhängig vom Startzeichen auswertet
    match = re.search(r'(?P<lat>\d{4}\.\d{2}[NS])\s*(?P<table_id>[/\\])(?P<lon>\d{5}\.\d{2}[EW])(?P<sym>.)', raw)
    if match:
        d = match.groupdict()
        try:
            lat = int(d['lat'][:2]) + float(d['lat'][2:7]) / 60
            if 'S' in d['lat']: lat = -lat
            lon = int(d['lon'][:3]) + float(d['lon'][3:8]) / 60
            if 'W' in d['lon']: lon = -lon
            
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
    host = 'euro.aprs2.net'
    port = 14580
    userline = f'user {CALLSIGN} pass {PASSCODE} vers PythonCloudServer 3.7 filter {FILTER_RULE}\r\n'
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.sendall(userline.encode('utf-8'))
            buffer = ""
            while True:
                data = s.recv(2048).decode('utf-8', errors='ignore')
                if not data: break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if '>' in line and not line.startswith('#'):
                        callsign = line.split('>')[0]
                        info = parse_aprs(line)
                        if info:
                            info['time'] = time.time()
                            with STORAGE_LOCK:
                                ACTIVE_STATIONS[callsign] = info
        except:
            time.sleep(5)

class APRSHTTPHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        
        if parsed_url.path == '/data.json':
            query_params = parse_qs(parsed_url.query)
            max_age = int(query_params.get('history', ['300'])[0])
            
            now = time.time()
            with STORAGE_LOCK:
                expired = [k for k, v in ACTIVE_STATIONS.items() if now - v['time'] > max_age]
                for k in expired: del ACTIVE_STATIONS[k]
                payload = json.dumps(ACTIVE_STATIONS)
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(payload.encode('utf-8'))
            
        elif parsed_url.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(b"<h3>APRS Cloud Server online!</h3><p>Nutze /data.json fuer die Daten.</p>")
            
        else:
            self.send_response(404)
            self.end_headers()
            
    def log_message(self, format, *args):
        pass

if __name__ == '__main__':
    threading.Thread(target=aprs_stream_worker, daemon=True).start()
    
    port = int(os.environ.get('PORT', 8080))
    server_address = ('0.0.0.0', port)
    httpd = http.server.HTTPServer(server_address, APRSHTTPHandler)
    print(f"Cloud-Server gestartet auf Port {port}", flush=True)
    httpd.serve_forever()