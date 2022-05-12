import socket
import sys
import requests
import json

def sendData(c_socket):
    for index in range(0,100):
        c_socket.send(str(index).encode('utf-8'))

TCP_IP = "117.17.189.206"
TCP_PORT = 13000
conn = None
# create a socket object
s = socket.socket()
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Waiting for TCP connection...")
conn, addr = s.accept()

print(conn,addr)

print("Received request from: " + str(addr))

# Keep the stream data available
sendData(c)
