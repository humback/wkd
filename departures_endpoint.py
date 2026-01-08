from flask import Blueprint, request, jsonify
import json
import csv
import os
from datetime import datetime, timedelta
from src.gtfs_parser import GTFSParser
from zoneinfo import ZoneInfo


def get_direction_by_tripid(trip_id):
    # trip_id format: <number>_<something>
    try:
        num = int(trip_id.split("_")[0])
        if num % 2 == 0:
            return "Warszawa"
        else:
            return "Grodzisk/Milanówek"
    except Exception:
        return "unknown"


departures_bp = Blueprint("departures", __name__)


@departures_bp.route("/departures", methods=["GET"])
def departures():
    stop_id = request.args.get("stop_id")
    direction_param = request.args.get("direction")
    if not stop_id:
        return jsonify({"error": "stop_id parameter is required"}), 400
    if direction_param:
        direction_param = direction_param.strip().lower()
        if direction_param in ["warszawa", "wawa"]:
            direction_filter = "Warszawa"
        elif direction_param in [
            "grodzisk",
            "milanówek",
            "milanowek",
            "grodzisk/milanówek",
            "grodzisk/milanowek",
        ]:
            direction_filter = "Grodzisk/Milanówek"
        else:
            return jsonify(
                {"error": "Invalid direction parameter. Use 'Warszawa' or 'Grodzisk'"}
            ), 400
    else:
        direction_filter = None

    # 1. Parse calendar_dates.txt for today's valid trips
    today_str = datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%Y%m%d")
    calendar_path = os.path.join(
        os.path.dirname(__file__), "wkd_gtfs", "calendar_dates.txt"
    )
    valid_trips = set()
    with open(calendar_path, newline="", encoding="utf-8") as calfile:
        reader = csv.DictReader(
            calfile, fieldnames=["date", "service_id", "exception_type"]
        )
        for row in reader:
            if row["date"] == today_str and row["exception_type"] == "1":
                valid_trips.add(row["service_id"])

    # 2. Parse stops.txt for stop_id -> stop_name mapping
    stops_path = os.path.join(os.path.dirname(__file__), "wkd_gtfs", "stops.txt")
    stop_names = {}
    with open(stops_path, newline="", encoding="utf-8") as stopsfile:
        reader = csv.DictReader(stopsfile)
        for row in reader:
            stop_names[row["stop_id"]] = row["stop_name"]

    # 3. Parse stop_times.txt and select next 5 trains per direction, only for valid trips
    stop_times_path = os.path.join(
        os.path.dirname(__file__), "wkd_gtfs", "stop_times.txt"
    )
    now = datetime.now(ZoneInfo("Europe/Warsaw"))
    schedule = {"Grodzisk/Milanówek": [], "Warszawa": []}
    # For each trip, find its last stop_id (destination)
    trip_dest = {}
    with open(stop_times_path, newline="", encoding="utf-8") as csvfile:
        reader = list(csv.DictReader(csvfile))
        # Build trip_id -> last stop_id
        for row in reader:
            trip_id = row["trip_id"]
            stop_id_in_row = row["stop_id"]
            trip_dest.setdefault(trip_id, []).append(
                (int(row["stop_sequence"]), stop_id_in_row)
            )
        # Now, for each trip_id, get the stop_id with max stop_sequence
        trip_last_stop = {
            tid: sorted(stops, key=lambda x: x[0])[-1][1]
            for tid, stops in trip_dest.items()
        }

        # Now, filter for departures from requested stop_id
        for row in reader:
            if row["stop_id"] != stop_id:
                continue
            trip_id = row["trip_id"]
            if trip_id not in valid_trips:
                continue
            direction = get_direction_by_tripid(trip_id)
            dep_time_str = row["departure_time"]
            h, m, s = map(int, dep_time_str.split(":"))
            dep_time = now.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(hours=h, minutes=m, seconds=s)
            if dep_time >= now:
                dest_stop_id = trip_last_stop.get(trip_id)
                dest_stop_name = stop_names.get(dest_stop_id, dest_stop_id)
                schedule[direction].append(
                    {
                        "trip_id": trip_id,
                        "scheduled_departure": dep_time,
                        "destination_stop_id": dest_stop_id,
                        "destination_stop_name": dest_stop_name,
                    }
                )

    # Sort and take 5 next for each direction
    for direction in schedule:
        schedule[direction].sort(key=lambda x: x["scheduled_departure"])
        schedule[direction] = schedule[direction][:5]

    # Fetch GTFS-RT for real-time data and delays
    parser = GTFSParser("https://mkuran.pl/gtfs/wkd.pb")
    parser.update()
    rt_data = parser.to_json()
    import json as _json

    rt_json = _json.loads(rt_data)
    # Map: (trip_id, stop_id) -> real_time_epoch
    realtime_map = {}
    for entity in rt_json.get("entity", []):
        trip_update = entity.get("tripUpdate")
        if not trip_update:
            continue
        trip = trip_update.get("trip", {})
        trip_id = trip.get("tripId")
        stop_time_updates = trip_update.get("stopTimeUpdate", [])
        for stu in stop_time_updates:
            sid = stu.get("stopId")
            dep = stu.get("departure", {})
            dep_time = dep.get("time")
            if dep_time:
                realtime_map[(trip_id, sid)] = int(dep_time)

    def enrich(trip):
        trip_id = trip["trip_id"]
        sched_dt = trip["scheduled_departure"]
        sched_epoch = int(sched_dt.timestamp())
        rt_epoch = realtime_map.get((trip_id, stop_id))
        if rt_epoch:
            delay_min = int(round((rt_epoch - sched_epoch) / 60))
            from datetime import datetime as _dt

            rt_str = _dt.fromtimestamp(rt_epoch).strftime("%H:%M:%S")
        else:
            delay_min = None
            rt_str = None
        return {
            "trip_id": trip_id,
            "scheduled_departure": sched_dt.strftime("%H:%M:%S"),
            "realtime_departure": rt_str,
            "delay_min": delay_min,
            "destination_stop_id": trip["destination_stop_id"],
            "destination_stop_name": trip["destination_stop_name"],
        }

    if direction_filter:
        # Only return the selected direction
        filtered = schedule[direction_filter]
        result = {
            "stop_id": stop_id,
            "direction": direction_filter,
            "departures": [enrich(x) for x in filtered],
        }
    else:
        result = {
            "stop_id": stop_id,
            "Grodzisk_Milanowek": [enrich(x) for x in schedule["Grodzisk/Milanówek"]],
            "Warszawa": [enrich(x) for x in schedule["Warszawa"]],
        }
    return jsonify(result)
