from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import requests


class GTFSParser:
    def __init__(self, url):
        self.url = url
        self.feed = gtfs_realtime_pb2.FeedMessage()

    def update(self):
        response = requests.get(self.url)
        self.feed.ParseFromString(response.content)

    def to_json(self):
        return MessageToJson(self.feed)

    def print_json(self):
        print(self.to_json())

    def save_json(self, filename):
        with open(filename, "w", encoding="utf-8") as f:
            f.write(self.to_json())


if __name__ == "__main__":
    parser = GTFSParser("https://mkuran.pl/gtfs/wkd.pb")
    parser.update()
    # parser.print_json()
    parser.save_json("wkd_gtfs.json")
