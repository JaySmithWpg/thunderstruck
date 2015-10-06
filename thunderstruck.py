import json
import time

INPUT_FILE = '00.json'
OUTPUT_FILE = 'geo00.json'

def convert_to_geostrike(raw_strike):
    return {"type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [raw_strike["lat"], raw_strike["lon"]]},
            "properties": {
                "name": "Strike",
                "time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(raw_strike["time"]/1000000000))}}

geo_strikes = []
with open(INPUT_FILE, 'rt') as input_file:
    for strike_data in input_file:
        raw_strike = json.loads(strike_data)
        geo_strikes.append(convert_to_geostrike(raw_strike))

geo_file = {"type": "FeatureCollection",
            "features": geo_strikes}

with open(OUTPUT_FILE, 'wt') as output_file:
    output_file.write(json.dumps(geo_file))

