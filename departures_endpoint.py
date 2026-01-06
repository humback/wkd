from flask import Blueprint, request, jsonify
import json
from src.gtfs_parser import GTFSParser


def get_direction(trip_stop_ids, stop_id):
    if trip_stop_ids[-1] == "wsrod":
        return "Warszawa Śródmieście"
    elif trip_stop_ids[-1] == "grodz":
        return "Grodzisk Mazowiecki"
    else:
        return trip_stop_ids[-1]


departures_bp = Blueprint("departures", __name__)


@departures_bp.route("/departures", methods=["GET"])
def departures():
    stop_id = request.args.get("stop_id")
    if not stop_id:
        return jsonify({"error": "stop_id parameter is required"}), 400

    parser = GTFSParser("https://mkuran.pl/gtfs/wkd.pb")
    parser.update()
    data = json.loads(parser.to_json())

    departures_by_direction = {}
    for entity in data.get("entity", []):
        trip_update = entity.get("tripUpdate")
        if not trip_update:
            continue
        trip = trip_update.get("trip", {})
        trip_id = trip.get("tripId")
        stop_time_updates = trip_update.get("stopTimeUpdate", [])
        for idx, stu in enumerate(stop_time_updates):
            if stu.get("stopId") == stop_id:
                direction = get_direction(
                    [s["stopId"] for s in stop_time_updates], stop_id
                )
                departure_time = stu.get("departure", {}).get("time")
                if direction not in departures_by_direction:
                    departures_by_direction[direction] = []
                departures_by_direction[direction].append(
                    {
                        "trip_id": trip_id,
                        "departure_time": int(departure_time)
                        if departure_time
                        else None,
                    }
                )
                break

    for direction in departures_by_direction:
        departures_by_direction[direction].sort(key=lambda x: x["departure_time"] or 0)

    result = {
        "stop_id": stop_id,
        "departures": [
            {"direction": direction, "departures": departures_by_direction[direction]}
            for direction in departures_by_direction
        ],
    }
    return jsonify(result)
