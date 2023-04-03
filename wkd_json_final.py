# %%
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import requests
import pytz
import json
import pandas as pd
from datetime import datetime
from datetime import date

rozklad = pd.read_csv(r'./wkd_gtfs/stop_times.txt', sep=',')
feed = gtfs_realtime_pb2.FeedMessage()
url = 'https://mkuran.pl/gtfs/wkd.pb'

def scheduleDateTime(stop_time):
    dt=date.today()
    time_obj = datetime.strptime(stop_time, "%H:%M:%S").time()
    dt_with_time = datetime.combine(dt, time_obj)
    dt_with_time = pytz.timezone('Europe/Warsaw').localize(dt_with_time)
    return dt_with_time
def gtfsRtUpdate(stop_id, direction):
    response = requests.get(url)
    feed.ParseFromString(response.content)

    json_wkd = MessageToJson(feed)
    json_wkd=json.loads(json_wkd)
    json_wkd=json_wkd['entity']

    df = pd.json_normalize(json_wkd, record_path=['tripUpdate','stopTimeUpdate'],meta=['id'])
    tz = pytz.timezone('Europe/Warsaw')
    df['departure.time'] = pd.to_datetime(df['departure.time'], unit='s').apply(lambda x: pd.Timestamp(x).tz_localize(None).tz_localize('UTC').tz_convert(tz))
    actual = df[df['stopId'] == stop_id]
    merged_df = pd.merge(actual, rozklad, left_on=['id','stopId'], right_on=['trip_id','stop_id'], how='left')
    merged_df = merged_df.drop(columns=['trip_id','stop_id','departure_time'])
    
    merged_df['arrival_time'] = merged_df['arrival_time'].fillna(merged_df['departure.time'].apply(lambda x: x.strftime('%H:%M:%S')))
    merged_df['arrival_time'] = merged_df['arrival_time'].apply(scheduleDateTime)
    merged_df['delay']=merged_df['departure.time']-merged_df['arrival_time']
    merged_df['delay'] = merged_df['delay'].apply(lambda x: x.total_seconds() / 60)
    merged_df['direction'] = merged_df['id'].apply(lambda x: 'W' if int(x[2:]) % 2 == 0 else 'P')
    merged_df=merged_df[['id','stopId','direction','delay','arrival_time','departure.time']]
    
    mask = merged_df['direction'] == direction
    merged_df=merged_df[mask]
    #merged_df.set_index('id',inplace=True)
    merged_df.rename(columns = {'departure.time':'real_departure_time'}, inplace = True)
    merged_df.sort_values(by='departure_time', inplace=True)
    #return merged_df.to_dict(orient='index')
    merged_df.drop_duplicates()
    
    print(merged_df)
    print(merged_df.departure_time.dtype)
    return merged_df

# %%
if __name__ == "__main__":
    delay_df= gtfsRtUpdate('nwwar','P')
    print(delay_df)

# %%





