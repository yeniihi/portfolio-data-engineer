
from kafka import KafkaConsumer
import psycopg
import json
from datetime import datetime

conn = psycopg.connect("dbname=brawlstars user=postgres password=Postgres!120 host=localhost port=5432")
cursor = conn.cursor()
cursor.execute("SET search_path TO brawlstars;")

consumer = KafkaConsumer(
    'ranking_clubs',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Club ranking consumer started at", datetime.now())
snapshot_cache = {}

for message in consumer:
    data = message.value
    snapshot_info = data.pop('snapshot')
    snapshot_key = (snapshot_info['region'], snapshot_info['type'], snapshot_info['collected_at'])

    if snapshot_key not in snapshot_cache:
        cursor.execute("""
            INSERT INTO ranking_snapshot (region, type, collected_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (snapshot_info['region'], snapshot_info['type'], snapshot_info['collected_at']))
        snapshot_id = cursor.fetchone()[0]
        snapshot_cache[snapshot_key] = snapshot_id
        conn.commit()
    else:
        snapshot_id = snapshot_cache[snapshot_key]

    try:
        cursor.execute("""
            INSERT INTO ranking_item (snapshot_id, rank, tag, name, trophies, club_name,
                                      icon_id, name_color, member_count, badge_id)
            VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL, %s, %s)
            ON CONFLICT (snapshot_id, rank) DO NOTHING
        """, (
            snapshot_id, data.get('rank'), data.get('tag'), data.get('name'),
            data.get('trophies'), data.get('memberCount'), data.get('badgeId')
        ))
        conn.commit()
    except Exception as e:
        print("Error inserting club ranking item:", e)

cursor.close()
conn.close()
