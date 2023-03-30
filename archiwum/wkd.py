from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import urllib.request
import datetime
import json

feed = gtfs_realtime_pb2.FeedMessage()
response = urllib.request.urlopen('https://mkuran.pl/gtfs/wkd.pb')

feed.ParseFromString(response.read())
print(type(feed.entity))
print(feed.header)
tx = feed.entity[0].trip_update.stop_time_update[0].departure.time

ts = datetime.datetime.fromtimestamp(tx)
print(ts.strftime('%Y-%m-%d %H:%M:%S'))

print(tx)
print(type(tx))
print('-----------------------------------')
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print(entity.trip_update)


json_obj = MessageToJson(feed)
print(json_obj)