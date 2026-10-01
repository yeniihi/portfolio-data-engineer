
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

def send_club_rankings():
    print("Fetching club rankings at", datetime.now())
    snapshot = {
        "region": REGION,
        "type": "clubs",
        "collected_at": datetime.now().isoformat()
    }
    clubs = client.get_rankings(region=REGION, ranking='clubs')
    for club in clubs.raw_data:
        club['snapshot'] = snapshot
        producer.send('ranking_clubs', club)
    producer.flush()

send_club_rankings()
