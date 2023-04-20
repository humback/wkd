import csv
import json
import io

def stationList():
    with io.open('wkd_gtfs/stops.txt', 'r', encoding='utf8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # get the column names from the first row
        data = [dict(zip(headers, row)) for row in reader]

    json_data = json.dumps(data, ensure_ascii=False) # convert list to JSON string
    return json_data