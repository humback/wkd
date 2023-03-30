from flask import Flask
from flask import jsonify
import wkd_json_final
app = Flask(__name__);

@app.route('/', methods=['GET'])
def hello():
    return wkd_json_final.gtfsRtUpdate('nwwar','P')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105)