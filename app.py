import socket
import re
import threading
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import psycopg2

# ==========================================
# KONFIGURATION
# ==========================================
APRS_SERVER = "euro.aprs2.net"
APRS_PORT = 14580
CALLSIGN = "N0CALL"  # Nur zum Handshake/Lesen benötigt
PASSCODE = "-1"

# APRS-IS Filter: s/R (Wohnmobile/Campers), s/Y (Yachten/Boote), s/s (Schiffe)
APRS_FILTER = "filter s/R s/Y s/s\r\n"

# Datenbank-URL aus Render.com Umgebungsvariablen
DATABASE_URL = os.environ.get("DATABASE_URL")

# In-Memory Speicher für die aktuellsten Live-Positionen
latest_stations = {}
lock = threading.Lock()

# Regex für Koordinaten & Symbole (! oder = oder @ gefolgt von Position)
APRS_REGEX = re.compile(
    r'([A-Z0-9\-]+)>.*?:[@=!]([0-9]{4}\.[0-9]{2}[NS])([/\\].)([0-9]{5}\.[0-9]{2}[EW])(.)(.*)'
)

# ==========================================
# DATENBANK-FUNKTIONEN (SUPABASE)
# ==========================================
def save_to_supabase(callsign, lat, lon, symbol, comment):
    """Speichert eine empfangene Bake dauerhaft in Supabase."""
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
            (callsign, lat, lon, symbol, comment[:100]) # Kommentar auf 100 Zeichen begrenzen
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB-ERROR] Fehler beim Schreiben in Supabase: {e}")

def get_history_from_supabase(callsign, hours=24):
    """Liest die Historie der letzten X Stunden aus Supabase aus."""
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
        return [
            {
                "lat": r[0],
                "lon": r[1],
                "time": r[2].isoformat(),
                "symbol": r[3]
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[DB-ERROR] Fehler beim Lesen aus Supabase: {e}")
        return []

# ==========================================
# APRS PARSER & SOCKET CLIENT
# ==========================================
def parse_degrees(coord_str, is_lat=True):
    """Konvertiert APRS-Gradminuten in Dezimalgrad."""
    try:
        if is_lat:
            deg = float(coord_str[:2])
            minutes = float(coord_str[2:-1])
            direction = coord_str[-1]
            val = deg + (minutes / 60.0)
            return -val if direction == 'S' else val
        else:
            deg = float(coord_str[:3])
            minutes = float(coord_str[3:-1])
            direction = coord_str[-1]
            val = deg + (minutes / 60.0)
            return -val if direction == 'W' else val
    except Exception:
        return None

def aprs_client_thread():
    """Hält die TCP-Verbindung zum APRS-IS aufrecht und verarbeitet Baken."""
    while True:
        try:
            print(f"[APRS] Verbinde mit {APRS_SERVER}:{APRS_PORT}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((APRS_SERVER, APRS_PORT))
            
            # Handshake
            auth = f"user {CALLSIGN} pass {PASSCODE} vers FunkerkameradenPortal 1.0 {APRS_FILTER}"
            s.sendall(auth.encode('utf-8'))
            
            buffer = ""
            while True:
                data = s.recv(4096).decode('utf-8', errors='ignore')
                if not data:
                    break
                
                buffer += data
                lines = buffer.split('\r\n')
                buffer = lines.pop() # Unvollständige letzte Zeile im Puffer halten
                
                for line in lines:
                    if line.startswith('#'):
                        continue
                    
                    match = APRS_REGEX.search(line)
                    if match:
                        station_call = match.group(1)
                        lat_raw = match.group(2)
                        table_char = match.group(3)
                        lon_raw = match.group(4)
                        symbol_char = match.group(5)
                        comment = match.group(6).strip()
                        
                        lat = parse_degrees(lat_raw, is_lat=True)
                        lon = parse_degrees(lon_raw, is_lat=False)
                        symbol = table_char + symbol_char
                        
                        if lat is not None and lon is not None:
                            # 1. In-Memory Puffer für Live-Karte aktualisieren
                            with lock:
                                latest_stations[station_call] = {
                                    "callsign": station_call,
                                    "lat": lat,
                                    "lon": lon,
                                    "symbol": symbol,
                                    "comment": comment
                                }
                            
                            # 2. Dauerhaft in Supabase speichern
                            threading.Thread(
                                target=save_to_supabase,
                                args=(station_call, lat, lon, symbol, comment),
                                daemon=True
                            ).start()

        except Exception as e:
            print(f"[APRS-ERROR] Verbindungsabbruch: {e}. Neuvorbindung in 10s...")
            import time
            time.sleep(10)

# ==========================================
# REST-API SERVER (CORS PREFLIGHT SUPPORT)
# ==========================================
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
        
        # Endpunkt 1: Live-Daten aller aktiven Stationen (/data.json)
        if parsed_path.path == '/data.json':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            
            with lock:
                data = list(latest_stations.values())
            self.wfile.write(json.dumps(data).encode('utf-8'))
            
        # Endpunkt 2: Historie einer bestimmten Station (/history.json?callsign=DL4KAL-7&hours=24)
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
    print(f"[HTTP] REST-API läuft auf Port {port}...")
    httpd.serve_forever()

# ==========================================
# HAUPTPROGRAMM
# ==========================================
if __name__ == '__main__':
    # Thread 1: APRS-IS Socket Listener
    aprs_thread = threading.Thread(target=aprs_client_thread, daemon=True)
    aprs_thread.start()
    
    # Hauptthread: HTTP Server für Render.com
    run_http_server()