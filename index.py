from flask import Flask
import wkd_json_final
import station_list
from departures_endpoint import departures_bp


app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


from departures_endpoint import departures_bp

app.register_blueprint(departures_bp)


# @app.route("/delay", methods=["GET"])
# def hello():
#     station = request.args.get("station")
#     direction = request.args.get("dir")
#     df = wkd_json_final.gtfsRtUpdate(station, direction)
#     return jsonify(df)


@app.route("/", methods=["GET"])
def home():
    return "Helllo!"


@app.route("/stations", methods=["GET"])
def stations():
    stations = station_list.stationList()
    return stations


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5105, debug=True)
