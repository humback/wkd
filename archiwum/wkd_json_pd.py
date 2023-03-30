from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import urllib.request
import datetime
import json
import pandas as pd

feed = gtfs_realtime_pb2.FeedMessage()
response = urllib.request.urlopen('https://mkuran.pl/gtfs/wkd.pb')

feed.ParseFromString(response.read())

#ts = datetime.datetime.fromtimestamp(tx)
#print(ts.strftime('%Y-%m-%d %H:%M:%S'))


json_wkd = MessageToJson(feed)
#print(json_obj)

df = pd.read_json(json_wkd)
print(df)