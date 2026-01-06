from flask import Blueprint, request, jsonify
import json
from src.gtfs_parser import GTFSParser


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
    if not stop_id:
        return jsonify({"error": "stop_id parameter is required"}), 400

    parser = GTFSParser("https://mkuran.pl/gtfs/wkd.pb")
    parser.update()
    data = json.loads(parser.to_json())

    directions = {"Grodzisk/Milanówek": [], "Warszawa": []}

    for entity in data.get("entity", []):
        trip_update = entity.get("tripUpdate")
        if not trip_update:
            continue
        trip = trip_update.get("trip", {})
        trip_id = trip.get("tripId")
        stop_time_updates = trip_update.get("stopTimeUpdate", [])
        for idx, stu in enumerate(stop_time_updates):
            if stu.get("stopId") == stop_id:
                direction = get_direction_by_tripid(trip_id)
                departure_time = stu.get("departure", {}).get("time")
                directions[direction].append(
                    {
                        "trip_id": trip_id,
                        "departure_time": int(departure_time)
                        if departure_time
                        else None,
                    }
                )
                break

    # Sort departures in each direction by time
    for direction in directions:
        directions[direction].sort(key=lambda x: x["departure_time"] or 0)

    result = {
        "stop_id": stop_id,
        "Grodzisk_Milanowek": directions["Grodzisk/Milanówek"],
        "Warszawa": directions["Warszawa"],
    }
    return jsonify(result)
