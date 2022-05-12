import argparse
import socket
import requests
import os
import json
from kafka import KafkaProducer
from urllib3.exceptions import ProtocolError
import datetime
import os
import time
from itertools import repeat
import multiprocessing
import multiprocessing.pool
import time
from random import randint

class Filteredstream(object):
    def __init__(self, bearer_token, query,addr,conn):
        self.addr = addr
        self.conn = conn
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


    def get_stream(self,  set):
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
                json_response['start_timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                print((json_response))
                try:
                    self.conn.send((json.dumps(json_response)+"\n").encode('utf-8'))
                except Exception as es:
                    print("xxxxx",es)
                    continue
                try:
                    self.producer.send("tweet", json.dumps(json_response).encode('utf-8'))
                    self.producer.flush()
                except Exception as es:
                    print(es)
                    self.producer.send("tweet", json.dumps(json_response).encode('utf-8'))
                    self.producer.flush()



    def start_api(self):
        while True:
            try:
                self.rules = self.get_rules()
                self.delete = self.delete_all_rules(self.rules)
                set = self.set_rules(self.delete)
                self.get_stream( set)
            except Exception as es:
                print("retry",es)
                continue



def execute(token, query,conn,addr):
    try:
        filtered_stream = Filteredstream(token, query,addr,conn)
        filtered_stream.start_api()
        print("query = ",query, "start",addr,conn) 
    except Exception as ex:
        pass



if __name__ == "__main__":
    print("start") 
    start=time.time()
    
    ## token_list
    with open('token_list.txt', 'r') as f:
        token_list_txt = f.read().split(",")
    
    token_list =[]
    for token in token_list_txt:
        token=token.strip()
        token_list.append(token)

    num_of_token = len(token_list)
    
    # query_list
    with open('query_list.txt', 'r') as f:
        query_list_txt = f.read().split(",")
    
    query_list =[]
    for query in query_list_txt:
        query=query.strip()
        query_list.append(query)
    
    
    TCP_IP = "117.17.189.206"
    TCP_PORT = 13000
    conn = None
    # create a socket object
    s = socket.socket()
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("listen") 
    
    conn, addr = s.accept()
    
    print(conn, addr)
    
    process_pool = multiprocessing.Pool(processes = num_of_token)
    process_pool.starmap(execute, zip(token_list,query_list,repeat(conn),repeat(addr)))
    process_pool.close()
    process_pool.join()

print("-------%s seconds -----"%(time.time()-start))
