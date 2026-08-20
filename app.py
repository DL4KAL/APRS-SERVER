import threading
import os
import time
from flask import Flask, jsonify, request
import psycopg2
import aprslib

app = Flask(__name__)

# ==========================================
# KONFIGURATION
# ==========================================
CALLSIGN = "DL4KAL"
PASSCODE = "17065"

# Filter: 500 km Umkreis um Bonn
APRS_FILTER = "filter r/50.7/7.1/500"
DATABASE_URL = os.environ.get("DATABASE_URL")

latest_stations = {}
lock = threading.Lock()

def log(msg):
    print(str(msg), flush=True)

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

def process_packet(packet):
    """Callback-Funktion fuer jedes erfolgreich von aprslib empfangene Paket."""
    try:
        if 'latitude' in packet and 'longitude' in packet:
            cs = packet.get('from', 'UNKNOWN')
            lat = packet['latitude']
            lon = packet['longitude']
            
            sym_table = packet.get('symbol_table', '/')
            sym_code = packet.get('symbol', '>')
            symbol = f"{sym_table}{sym_code}"
            
            comment = packet.get('comment', '')
            
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
        pass

def aprs_client_thread():
    log("==========================================")
    log("=== APRSLIB CLIENT THREAD GESTARTET ===")
    log("==========================================")
    while True:
        try:
            AIS = aprslib.IS(CALLSIGN, passwd=PASSCODE, host="euro.aprs2.net", port=14580)
            AIS.set_filter(APRS_FILTER)
            AIS.connect()
            log("[APRS] Erfolgreich mit euro.aprs2.net verbunden!")
            
            # Startet die Endlosschleife zum Verarbeiten der Pakete
            AIS.consumer(process_packet, raw=False)
            
        except Exception as e:
            log(f"[APRS-ERROR] Verbindungsverlust: {e}. Reconnect in 5s...")
            time.sleep(5)

# Thread sofort starten
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
