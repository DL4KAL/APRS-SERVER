import socket
import threading
import json
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import psycopg2
import aprslib

# ==========================================
# KONFIGURATION
# ==========================================
APRS_SERVER = "euro.aprs2.net"
APRS_PORT = 14580
CALLSIGN = "N0CALL"
PASSCODE = "-1"

# APRS-IS Filter: Umkreis 500km um Bonn (50.7° N, 7.1° E) für verlässlichen Datenstrom
APRS_FILTER = "filter r/50.7/7.1/500"

DATABASE_URL = os.environ.get("DATABASE_URL")

latest_stations = {}
lock = threading.Lock()

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
            (callsign, lat, lon, symbol, comment[:100])
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB-ERROR] Schreiben fehlgeschlagen: {e}")

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
        print(f"[DB-ERROR] Lesen fehlgeschlagen: {e}")
        return []

# ==========================================
# APRS-IS CLIENT (MIT APRSLIB PARSER)
# ==========================================
def aprs_client_thread():
    """Verbindet sich mit APRS-IS und parst Baken robust via aprslib."""
    while True:
        try:
            print(f"[APRS] Verbinde mit {APRS_SERVER}:{APRS_PORT}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((APRS_SERVER, APRS_PORT))
            
            # Handshake senden
            auth = f"user {CALLSIGN} pass {PASSCODE} vers FunkerkameradenPortal 1.1\r\n"
            s.sendall(auth.encode('utf-8'))
            
            # 1 Sekunde warten, dann Filter separat absetzen
            time.sleep(1)
            filter_cmd = f"{APRS_FILTER}\r\n"
            s.sendall(filter_cmd.encode('utf-8'))
            print(f"[APRS] Filter gesendet: {APRS_FILTER}")
            
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
                    
                    try:
                        # aprslib übernimmt das vollständige Decoden (inkl. MIC-E)
                        packet = aprslib.parse(line)
                        
                        if 'latitude' in packet and 'longitude' in packet:
                            cs = packet.get('from', 'UNKNOWN')
                            lat = packet['latitude']
                            lon = packet['longitude']
                            
                            sym_table = packet.get('symbol_table', '/')
                            sym_code = packet.get('symbol', '>')
                            symbol = f"{sym_table}{sym_code}"
                            
                            comment = packet.get('comment', '')
                            
                            # 1. In-Memory-Speicher für Live-Daten
                            with lock:
                                latest_stations[cs] = {
                                    "callsign": cs,
                                    "lat": lat,
                                    "lon": lon,
                                    "symbol": symbol,
                                    "comment": comment
                                }
                            
                            # 2. Asynchron in Supabase sichern
                            threading.Thread(
                                target=save_to_supabase,
                                args=(cs, lat, lon, symbol, comment),
                                daemon=True
                            ).start()
                            
                    except (aprslib.ParseError, aprslib.UnknownFormat):
                        # Nicht parsebare Baken ignorierten
                        pass
                    except Exception as parse_ex:
                        pass

        except Exception as e:
            print(f"[APRS-ERROR] Verbindungsabbruch: {e}. Reconnect in 10s...")
            time.sleep(10)

# ==========================================
# REST-API SERVER
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
        
        # Endpunkt /data.json -> Liefert alle Baken im Puffer
        if parsed_path.path == '/data.json':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            
            with lock:
                data = list(latest_stations.values())
            self.wfile.write(json.dumps(data).encode('utf-8'))
            
        # Endpunkt /history.json -> Liefert den Streckenverlauf aus Supabase
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
    print(f"[HTTP] REST-API laeuft auf Port {port}...")
    httpd.serve_forever()

# ==========================================
# HAUPTPROGRAMM
# ==========================================
if __name__ == '__main__':
    aprs_thread = threading.Thread(target=aprs_client_thread, daemon=True)
    aprs_thread.start()
    
    run_http_server()
