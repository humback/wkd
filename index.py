import datetime
from flask import Flask, request
from flask import jsonify
from flask import Response
import pandas as pd
import wkd_json_final
import json
app = Flask(__name__);
app.config['JSON_SORT_KEYS'] = False


@app.route('/delay', methods=['GET'])
def hello():
    station=request.args.get('station')
    direction=request.args.get('dir')
    df = wkd_json_final.gtfsRtUpdate(station,direction)
    df=df.to_dict(orient="records")
    
    return jsonify(df)

@app.route('/delay2', methods=['GET'])
def hello():
    station=request.args.get('station')
    direction=request.args.get('dir')
    df = wkd_json_final.gtfsRtUpdate(station,direction)
    df=df.to_dict(orient="records")
    df = {"root":df}
    df["update"]=datetime.datetime.utcnow()
    return jsonify(df)

@app.route('/', methods=['GET'])
def home():
    
    return "Helllo!"


#if __name__ == '__main__':
   # app.run(host='0.0.0.0', port=5105,debug=True)
    