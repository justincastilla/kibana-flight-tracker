import socket
import json
from elasticsearch import Elasticsearch, helpers
import time


es = Elasticsearch(
    hosts="<YOUR ELASTICSEARCH HOST HERE>",
    api_key="<YOUR API KEY HERE",
)

INDEX_NAME = "adsb-traffic"

mapping = {
    "mappings": {
        "properties": {
            "icao": {"type": "keyword"},
            "flight": {"type": "keyword"},
            "altitude": {"type": "integer"},
            "heading": {"type": "integer"},
            "location": {"type": "geo_point"},
            "timestamp": {"type": "date"},
            "speed": {"type": "integer"},
        }
    }
}

# refresh index every reload for debug
print(es.options(ignore_status=400).indices.delete(index=INDEX_NAME))
print(es.options(ignore_status=400).indices.create(index=INDEX_NAME, body=mapping))


# --- Function to parse SBS1 CSV lines ---
def parse_sbs1(line):
    fields = line.strip().split(",")
    if len(fields) < 22:
        return None
    if fields[0] != "MSG":
        return None

    timestamp = int((time.time()) * 1000)

    speed = int(fields[12]) if fields[12] else 0
    heading = int(fields[13]) if fields[13] else 0

    # Use default 0.0 if missing lat/lon
    lat = float(fields[14]) if fields[14] else 0.0
    lon = float(fields[15]) if fields[15] else 0.0
    icao = fields[4]

    return {
        "icao": icao,
        "flight": fields[10].strip() or None,
        "altitude": int(fields[11]) if fields[11] else None,
        "speed": speed,
        "heading": heading,
        "lat": lat,
        "lon": lon,
        "location": {
            "lat": lat,
            "lon": lon,
        },
        "timestamp": timestamp,
    }


def format_update(doc):
    update_doc = {}

    if doc["lat"] != 0.0 and doc["lon"] != 0.0:
        update_doc["lat"] = doc["lat"]
        update_doc["lon"] = doc["lon"]
        update_doc["location"] = doc["location"]

    if doc["speed"] != 0.0:
        update_doc["speed"] = doc["speed"]

    if doc["heading"] != 0.0:
        update_doc["heading"] = doc["heading"]

    if doc["altitude"] is not None:
        update_doc["altitude"] = doc["altitude"]

    if doc["flight"]:
        update_doc["flight"] = doc["flight"]

    update_doc["timestamp"] = doc["timestamp"]
    update_doc["speed"] = doc["speed"]
    update_doc["icao"] = doc["icao"]

    return {
        "_op_type": "update",
        "_index": INDEX_NAME,
        "_id": doc["icao"],
        "doc": update_doc,
        "doc_as_upsert": True,
        "upsert": {
            "icao": doc["icao"],
            "location": {"lat": doc["lat"], "lon": doc["lon"]},
            "altitude": doc["altitude"],
            "heading": doc["heading"],
            "speed": doc["speed"],
            "flight": doc["flight"],
            "timestamp": doc["timestamp"],
        },
    }


def bulk_send(docs):
    if not docs:
        return

    actions = []
    for doc in docs:
        doc = format_update(doc)

        actions.append(doc)

    try:
        response = helpers.bulk(es, actions)
        print(f"Bulk index response: {response}")
    except helpers.BulkIndexError as e:
        for error in e.errors:
            print(json.dumps(error, indent=2))


HOST = "localhost"
PORT = 30003  # SBS1 TCP output

try:
    print("Connecting to dump1090...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
except socket.error as e:
    print(f"Error connecting to dump1090: {e}")
    exit(1)
print("Connected to dump1090.")


buffer = []
BULK_SIZE = 50  # Tune based on load

try:
    while True:
        data = sock.recv(1024).decode("utf-8")
        lines = data.strip().split("\n")

        for line in lines:
            doc = parse_sbs1(line)
            if doc:
                buffer.append(doc)

            if len(buffer) >= BULK_SIZE:
                bulk_send(buffer)
                buffer.clear()

except KeyboardInterrupt:
    print("Stopping ingestion.")
    bulk_send(buffer)  # Final flush

finally:
    sock.close()
