def save_to_db(callsign, lat, lon, symbol, comment):
    """Speichert jede empfangene Bake dauerhaft in Supabase"""
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
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
        print(f"[DB-WRITE-ERROR] {e}", flush=True)

def get_history_from_db(callsign=None, hours=24):
    """Liest historische Punkte der letzten X Stunden aus Supabase"""
    if not DATABASE_URL:
        return []
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        cur = conn.cursor()
        
        if callsign:
            cur.execute(
                """
                SELECT callsign, latitude, longitude, symbol, timestamp 
                FROM aprs_history 
                WHERE callsign ILIKE %s AND timestamp >= NOW() - (%s || ' hours')::INTERVAL
                ORDER BY timestamp ASC
                """,
                (f"%{callsign}%", str(hours))
            )
        else:
            cur.execute(
                """
                SELECT callsign, latitude, longitude, symbol, timestamp 
                FROM aprs_history 
                WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
                ORDER BY timestamp ASC
                """,
                (str(hours),)
            )
            
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"callsign": r[0], "lat": r[1], "lon": r[2], "symbol": r[3], "time": r[4].isoformat()} for r in rows]
    except Exception as e:
        print(f"[DB-READ-ERROR] {e}", flush=True)
        return []
