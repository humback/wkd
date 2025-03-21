# %%
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import requests
import pytz
import json
import datetime
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
    #print (dt_with_time)
    return dt_with_time
def get_direction(x):
    
    try:
       
        #num = int(x.split("D-")[1])
        #num = int(x[2:10])
        num = int(x.split('_')[0])
        print("---------------------"+str(num))
        return 'W' if num % 2 == 0 else 'P'
    except (ValueError, IndexError):
        return None

def gtfsRtUpdate(stop_id, direction):
    response = requests.get(url)
    
    feed.ParseFromString(response.content)
    #print(response.content)
    json_wkd = MessageToJson(feed)
    #print(json_wkd)
    json_wkd=json.loads(json_wkd)
    #print(json_wkd)
    json_wkd=json_wkd['entity']
    print (json_wkd)
    df = pd.json_normalize(json_wkd, record_path=['tripUpdate','stopTimeUpdate'],meta=['id'])
   
    tz = pytz.timezone('Europe/Warsaw')
    df['departure.time'] = pd.to_datetime(df['departure.time'], unit='s').apply(lambda x: pd.Timestamp(x).tz_localize(None).tz_localize('UTC').tz_convert(tz))
    actual = df[df['stopId'] == stop_id]
    #wyciannie daty z id
    
    print("-----------------------------------------------------------------")
    print (actual)
    #actual["id"] = actual["id"].apply(lambda x: x[11:])
    print("-----------------------------------------------------------------")
    print (actual)
    print (rozklad)
    merged_df = pd.merge(actual, rozklad, left_on=['id','stopId'], right_on=['trip_id','stop_id'], how='left')
    print ("XX",merged_df)
    merged_df = merged_df.drop(columns=['trip_id','stop_id','departure_time'])
    
    merged_df['arrival_time'] = merged_df['arrival_time'].fillna(merged_df['departure.time'].apply(lambda x: x.strftime('%H:%M:%S')))
    merged_df['arrival_time'] = merged_df['arrival_time'].apply(scheduleDateTime)
    merged_df['delay']=merged_df['departure.time']-merged_df['arrival_time']
    merged_df['delay'] = merged_df['delay'].apply(lambda x: x.total_seconds() / 60)
    
    #merged_df['id'] = merged_df['id'].apply(lambda x: x.split(":")[1])
    merged_df['direction'] = merged_df['id'].apply(get_direction)
    merged_df=merged_df[['id','stopId','direction','delay','arrival_time','departure.time']]
    print (f"merged_df 1:{merged_df}")
    #merged_df['arrival_time'] = merged_df['arrival_time'].apply(lambda x: x.int(datetime_obj.timestamp()))
    
    mask = merged_df['direction'] == direction
    merged_df=merged_df[mask]
    print (mask)
    print (f"merged_df 2:{merged_df}")
    merged_df.rename(columns = {'departure.time':'realDepartureTime','arrival_time':'scheduleDepartureTime'}, inplace = True)
    merged_df.sort_values(by='realDepartureTime', inplace=True)
    

    merged_df = merged_df.drop(columns=['direction','stopId'])
    
    print (" *************************")
    merged_df.drop_duplicates()
    merged_dict=merged_df.to_dict(orient="records")
    merged_dict = {"trains":merged_dict}
    merged_dict["update"]=datetime.utcnow()
    merged_dict["direction"]=direction
    merged_dict["station"]=stop_id
    
    return merged_dict

# %%
if __name__ == "__main__":
    delay_df= gtfsRtUpdate('nwwar','P')
    print(delay_df)

# %%





