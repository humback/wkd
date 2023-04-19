import json

import io

def stationList():
    with io.open('wkd_gtfs/stops.txt', 'r', encoding='utf8') as f:
        rows = [line.strip().split(',') for line in f][1:]  # skip first row and split values
    data = {}
    for row in rows:
        key = row.pop(0)
        data[key] = row
    json_data = json.dumps(data, ensure_ascii=False)
    return json_data