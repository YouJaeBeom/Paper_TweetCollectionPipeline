import argparse
import requests
import os
import json
from kafka import KafkaProducer
from urllib3.exceptions import ProtocolError


class Filteredstream(object):
    def __init__(self, bearer_token, query):
        self.bearer_token = bearer_token
        self.query = "%s -is:retweet"%(query)
        print("self.query",self.query)
        
        
    def bearer_oauth(self,r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = "Bearer {}".format(self.bearer_token)
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r


    def get_rules(self):
        print("start get_rules", self.bearer_token)
        self.response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearer_oauth
        )
        if self.response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(self.response.status_code, self.response.text)
            )
        print(json.dumps(self.response.json()))
        return self.response.json()


    def delete_all_rules(self,rules):
        print("start delete_all_rules", self.bearer_token)
        self.rules = rules 
        if rules is None or "data" not in rules:
            return None

        self.ids = list(map(lambda rule: rule["id"], self.rules["data"]))
        self.payload = {"delete": {"ids": self.ids}}
        self.response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=self.payload
        )
        if self.response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    self.response.status_code, self.response.text
                )
            )
        print(json.dumps(self.response.json()))


    def set_rules(self,delete):
        print("start set_rules", self.bearer_token)
        # You can adjust the rules if needed
        
        self.sample_rules = [
            {"value": self.query},
        ]
        self.payload = {"add": self.sample_rules}
        self.response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=self.payload,
        )
        if self.response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(self.response.status_code, self.response.text)
            )
        print(json.dumps(self.response.json()))


    def get_stream(self,set):
        print("start get_stream", self.bearer_token)
        self.producer = KafkaProducer(acks=0, compression_type='gzip', api_version=(0, 10, 1), bootstrap_servers=['117.17.189.205:9092','117.17.189.205:9093','117.17.189.205:9094'])

        self.response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at&expansions=referenced_tweets.id", auth=self.bearer_oauth, stream=True,
        )
        print(self.response.status_code)
        if self.response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    self.response.status_code, self.response.text
                )
            )
        for response_line in self.response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                try:
                    refer=str(json_response['data']['referenced_tweets'][0]['type'])
                    if str(refer) != "quoted":
                        self.producer.send("tweet_api", json.dumps(json_response).encode('utf-8'))
                        self.producer.flush()
                        print(json_response)
                        #print(json.dumps(json_response, sort_keys=True))
                except Exception as es:
                    self.producer.send("tweet_api", json.dumps(json_response).encode('utf-8'))
                    self.producer.flush()
                    print(json_response)
                    #print(json.dumps(json_response, sort_keys=True)) 



    def start_api(self):
        while True:
            try:
                self.rules = self.get_rules()
                self.delete = self.delete_all_rules(self.rules)
                set = self.set_rules(self.delete)
                self.get_stream(set)
            except Exception as es:
                print("retry",es)
                continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    
    parser.add_argument("--token",help="setting token")
    parser.add_argument("--query",help="setting query")

    args = parser.parse_args()
    
    filtered_stream = Filteredstream(args.token, args.query)
    filtered_stream.start_api()
