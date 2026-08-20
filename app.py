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
FILTER_RULE = "s/R"  # Weltweiter Filter für Wohnmobile
# Filter für Wohnmobile (R), Segelyachten (Y) und Motorboote (s)
FILTER_RULE = "s/R s/Y s/s"
# ==========================================

ACTIVE_STATIONS = {}
STORAGE_LOCK = threading.Lock()

def parse_aprs(raw):
    match = re.search(r'[!=](?P<lat>\d{4}\.\d{2}[NS])(?P<table_id>[/\\])(?P<lon>\d{5}\.\d{2}[EW])(?P<sym>.)(?:(?P<course>\d{3})/(?P<speed>\d{3}))?(?:/A=(?P<alt>\d{6}))?(?P<comment>.*)', raw)
    # Flexiblerer Regex, der APRS-Pakete unabhängig vom Startzeichen auswertet
    match = re.search(r'(?P<lat>\d{4}\.\d{2}[NS])\s*(?P<table_id>[/\\])(?P<lon>\d{5}\.\d{2}[EW])(?P<sym>.)', raw)
if match:
d = match.groupdict()
        lat = int(d['lat'][:2]) + float(d['lat'][2:7]) / 60
        if 'S' in d['lat']: lat = -lat
        lon = int(d['lon'][:3]) + float(d['lon'][3:8]) / 60
        if 'W' in d['lon']: lon = -lon
        
        return {
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "sym": d['table_id'] + d['sym'],
            "course": d['course'] or "0",
            "speed": d['speed'] or "0",
            "alt": int(d['alt']) if d['alt'] else 0,
            "comment": d['comment'].strip()
    }
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
    userline = f'user {CALLSIGN} pass {PASSCODE} vers PythonCloudServer 3.3 filter {FILTER_RULE}\r\n'
    userline = f'user {CALLSIGN} pass {PASSCODE} vers PythonCloudServer 3.7 filter {FILTER_RULE}\r\n'
while True:
try:
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
@@ -85,11 +90,10 @@
self.wfile.write(payload.encode('utf-8'))

elif parsed_url.path == '/':
            # WICHTIG FÜR RENDER: Beantwortet den Health-Check mit "OK"
self.send_response(200)
self.send_header('Content-Type', 'text/html; charset=utf-8')
self.end_headers()
            self.wfile.write(b"<h3>APRS Wohnmobil Server online!</h3><p>Nutze /data.json fuer die Daten.</p>")
            self.wfile.write(b"<h3>APRS Cloud Server online!</h3><p>Nutze /data.json fuer die Daten.</p>")

else:
self.send_response(404)
