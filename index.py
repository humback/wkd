from flask import Flask, request
from flask import jsonify
from flask import Response
import pandas as pd
#import wkd_json_final
app = Flask(__name__);


#@app.route('/delay', methods=['GET'])
#def hello():
 #   station=request.args.get('station')
  #  direction=request.args.get('dir')
   # df = wkd_json_final.gtfsRtUpdate(station,direction)
    #df=df.to_dict(orient="index")
    #return jsonify(df)

@app.route('/', methods=['GET'])
def home():
    
    return "Helllo!"


#if __name__ == '__main__':
   # app.run(host='0.0.0.0', port=5105,debug=True)
    