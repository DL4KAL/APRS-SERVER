import socket
import re
import threading
import os
import time
from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

# ==========================================
# KONFIGURATION
# ==========================================
APRS_SERVER = "euro.aprs2.net"
APRS_PORT = 14580
CALLSIGN = "DL4KAL"
PASSCODE = "17065"

# ERFORDERLICH: Zeilenumbruch \r\n erzwingt die Ausführung auf euro.aprs2.net
# t/po filtert weltweit alle Positionsbaken (Position Only)
APRS_FILTER = "filter t/po\r\n"

DATABASE_URL = os.environ.get("DATABASE_URL")

latest_stations = {}
lock = threading.Lock()

# Erweiterter Regex: Erfasst Standard-, MIC-E- (`/`) und komprimierte Baken
APRS_REGEX = re.compile(
    r'([A-Z0-9\-]+)>.*?:[@=!/\'`\`]([0-9]{4}\.[0-9]{2}[NS])([/\\].)([0-9]{5}\.[0-9]{2}[EW])(.)(.*)'
)

def log(msg):
    print(str(msg), flush=True)

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
        log(f"[DB-ERROR] {e}")

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
        log(f"[DB-ERROR] {e}")
        return []

def aprs_client_thread():
    log("==========================================")
    log("=== APRS THREAD MIT WILDCARD-FILTER LAEUFT ===")
    log("==========================================")
    while True:
        try:
            log(f"[APRS] Verbinde zu {APRS_SERVER}:{APRS_PORT}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(30.0)
            s.connect((APRS_SERVER, APRS_PORT))
            
            # Handshake mit inline-Filterabgabe
            login_str = f"user {CALLSIGN} pass {PASSCODE} vers DL4KAL-Portal 2.3 {APRS_FILTER}"
            s.sendall(login_str.encode('utf-8'))
            log("[APRS] Handshake und Filter t/po gesendet.")
            
            buffer = ""
            while True:
                data = s.recv(4096).decode('utf-8', errors='ignore')
                if not data:
                    log("[APRS-WARN] Keine Daten empfangen, Reconnect...")
                    break
                
                buffer += data
                lines = buffer.split('\r\n')
                buffer = lines.pop()
                
                for line in lines:
                    if not line or line.startswith('#'):
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
                            
                            log(f"[APRS-Empfang] {cs} @ {lat}, {lon}")
                            
                            threading.Thread(
                                target=save_to_supabase,
                                args=(cs, lat, lon, symbol, comment),
                                daemon=True
                            ).start()

        except Exception as e:
            log(f"[APRS-ERROR] {e}. Reconnect in 5s...")
            time.sleep(5)

aprs_thread = threading.Thread(target=aprs_client_thread, daemon=True)
aprs_thread.start()

# ==========================================
# FLASK REST-API ENDPUNKTE
# ==========================================
@app.route('/data.json', methods=['GET'])
def get_data():
    with lock:
        data = list(latest_stations.values())
    res = jsonify(data)
    res.headers.add('Access-Control-Allow-Origin', '*')
    return res

@app.route('/history.json', methods=['GET'])
def get_history():
    callsign = request.args.get('callsign')
    hours = request.args.get('hours', 24, type=int)
    
    if callsign:
        history_data = get_history_from_supabase(callsign, hours=hours)
        res = jsonify(history_data)
    else:
        res = jsonify({"error": "Parameter callsign fehlt"})
        res.status_code = 400
        
    res.headers.add('Access-Control-Allow-Origin', '*')
    return res

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
