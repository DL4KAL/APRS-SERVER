import socket
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

# Filter: 500 km Umkreis um Bonn
APRS_FILTER = "filter r/50.7/7.1/500\r\n"
DATABASE_URL = os.environ.get("DATABASE_URL")

latest_stations = {}
lock = threading.Lock()

def log(msg):
    print(str(msg), flush=True)

def parse_simple_aprs(line):
    """
    Extrem robuster Universal-Parser für APRS-Baken.
    Liest Rufzeichen, Breitengrad und Längengrad ohne komplexe Regex.
    """
    try:
        if '>' not in line or ':' not in line:
            return None, None, None, None, None
        
        callsign = line.split('>')[0].strip()
        body = line.split(':', 1)[1]
        
        # Suche nach Koordinaten-Muster DDMM.MMN/S und DDDMM.MME/W
        import re
        lat_match = re.search(r'([0-9]{4}\.[0-9]{2})([NS])', body)
        lon_match = re.search(r'([0-9]{5}\.[0-9]{2})([EW])', body)
        
        if lat_match and lon_match:
            lat_deg = float(lat_match.group(1)[:2])
            lat_min = float(lat_match.group(1)[2:])
            lat = lat_deg + (lat_min / 60.0)
            if lat_match.group(2) == 'S':
                lat = -lat
                
            lon_deg = float(lon_match.group(1)[:3])
            lon_min = float(lon_match.group(1)[3:])
            lon = lon_deg + (lon_min / 60.0)
            if lon_match.group(2) == 'W':
                lon = -lon
                
            symbol = "/="
            comment = body[:50]
            
            return callsign, lat, lon, symbol, comment
            
    except Exception:
        pass
    return None, None, None, None, None

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
            (callsign, lat, lon, symbol, str(comment)[:100])
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
    log("=== APRS DIRECT SOCKET LAEUFT ===")
    log("==========================================")
    while True:
        try:
            log(f"[APRS] Verbinde zu {APRS_SERVER}:{APRS_PORT}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(30.0)
            s.connect((APRS_SERVER, APRS_PORT))
            
            # Handshake
            login_str = f"user {CALLSIGN} pass {PASSCODE} vers DL4KAL-Portal 3.0\r\n"
            s.sendall(login_str.encode('utf-8'))
            
            time.sleep(1)
            s.sendall(APRS_FILTER.encode('utf-8'))
            log(f"[APRS] Filter gesendet: {APRS_FILTER.strip()}")
            
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
                    
                    cs, lat, lon, sym, comment = parse_simple_aprs(line)
                    
                    if cs and lat is not None and lon is not None:
                        with lock:
                            latest_stations[cs] = {
                                "callsign": cs,
                                "lat": lat,
                                "lon": lon,
                                "symbol": sym,
                                "comment": comment
                            }
                        
                        log(f"[APRS-Empfang] {cs} @ {lat}, {lon}")
                        
                        threading.Thread(
                            target=save_to_supabase,
                            args=(cs, lat, lon, sym, comment),
                            daemon=True
                        ).start()

        except Exception as e:
            log(f"[APRS-ERROR] {e}. Reconnect in 5s...")
            time.sleep(5)

# Thread direkt starten
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
