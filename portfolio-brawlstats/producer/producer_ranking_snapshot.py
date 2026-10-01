
import brawlstats
from kafka import KafkaProducer
import json
from datetime import datetime

API_KEY = 'YOUR_API_KEY_HERE'
REGION = 'global'
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

client = brawlstats.Client(API_KEY)

def send_rankings():
    print("Fetching rankings at", datetime.now())
    snapshot = {
        "region": REGION,
        "type": "players",
        "collected_at": datetime.now().isoformat()
    }
    rankings = client.get_rankings(region=REGION, ranking='players')
    for player in rankings.raw_data:
        player['snapshot'] = snapshot
        producer.send('ranking_players', player)
    producer.flush()

send_rankings()
