# debug_departures.py
from src.gtfs_parser import GTFSParser
import json


def debug_departures(stop_id):
    parser = GTFSParser("https://mkuran.pl/gtfs/wkd.pb")
    parser.update()
    data = json.loads(parser.to_json())

    all_trips = set()
    trips_with_departure = set()

    for entity in data.get("entity", []):
        trip_update = entity.get("tripUpdate")
        if not trip_update:
            continue
        trip = trip_update.get("trip", {})
        trip_id = trip.get("tripId")
        all_trips.add(trip_id)
        stop_time_updates = trip_update.get("stopTimeUpdate", [])
        for stu in stop_time_updates:
            if stu.get("stopId") == stop_id:
                trips_with_departure.add(trip_id)
                break

    print(f"Total trips in GTFS: {len(all_trips)}")
    print(f"Trips with departure at {stop_id}: {len(trips_with_departure)}")
    print(f"Missing trips: {all_trips - trips_with_departure}")


if __name__ == "__main__":
    debug_departures("nwwar")
