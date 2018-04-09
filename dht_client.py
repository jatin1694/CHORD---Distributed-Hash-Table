#!/usr/bin/env python

import socket
import time
import hashlib
import json
import argparse

#function for hashing the file name
def hashing(object_name):
    s = hashlib.sha1(object_name)
    iphex = s.hexdigest()
    identifier = int(iphex, 16)
    m = 10
    limit  = pow(2,m)
    nodeID = identifier%limit
    return nodeID

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-p", "--client_port")
    parser.add_argument("-h", "--client_hostname")
    parser.add_argument("-r", "--root_port")
    parser.add_argument("-R", "--root_hostname")
    args = parser.parse_args()

    host = args.client_hostname
    port = int(args.client_port)
    root_hostname = args.root_hostname
    root_port = int(args.root_port)
    recvSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    recvSocket.bind((host,port))
    recvSocket.listen(10)
    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sendSocket.connect((root_hostname, root_port))
    sendSocket.close()
    #checks if the client is connecting to the root successfully
    print "connection established successfully!"
    #the menu for the client
    while True:
            menu = '----------Menu----------\n' \
                   '1. Store Object (Press s)\n' \
                   '2. Retreive Obeject Iteratively (Press i)\n' \
                   '3. Retreive Object recursively (Press r)\n' \
                   '4. Exit \n'

            c=raw_input(menu)
            if c =='s':
                #code for storing the file in the peer network
                file_name = raw_input("Enter the file name?")
                objID = hashing(file_name)
                print objID
                data = {
                    'type' : 'file node identification',
                    'objID': objID,
                    'client_host' : host,
                    'client_port' : port
                }
                jsonData = json.dumps(data)
                sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sendSocket.connect((root_hostname, root_port))
                sendSocket.sendall(jsonData)
                sendSocket.close()
                print "data for file node identification sent to root"

                c, addr = recvSocket.accept()
                data = c.recv(2048)
                info = json.loads(data)
                if (info['type'] == 'file node ack'):
                    #received data about the node where the file is to be stored
                    print "data of file node received from node!"
                    with open(file_name,'rb') as fr:
                        data = {
                            'objID' : objID,
                            'type' : 'file storage',
                            'filename' : file_name,
                            'data' : fr.read()
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['destinationHost'], info['destinationPort']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        c.close()
                        print "File stored successfully!"

            elif c=='r':
                #code for looking up file recursively
                file_name=raw_input("Enter the name of the file?")
                objID = hashing(file_name)
                data = {
                    'type' : 'Lookup file Recursively',
                    'objID' : objID,
                    'client_host' : host,
                    'client_port' : port
                }
                jsonData = json.dumps(data)
                sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sendSocket.connect((root_hostname,root_port))
                sendSocket.sendall(jsonData)
                sendSocket.close()
                print "File name sent to the root"

                c, addr = recvSocket.accept()
                data = c.recv(2048)
                info = json.loads(data)
                if (info['type'] == 'Recursive Lookup Return'):
                    data = {
                        'type' : 'Recursive File Request',
                        'objID' : objID,
                        'client_host' : host,
                        'client_port' : port
                    }
                    print "Details of the node received by the root"
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((info['FileNodeHost'], info['FileNodePort']))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    c, addr = recvSocket.accept()
                    data = c.recv(2048)
                    if data:
                        with open(file_name,"wb") as fr:
                            fr.write(data)
                        print "File received successfully and stored!"

                elif info['type'] == 'File Not Found':
                    print "The entered file is not found in the system!Please try again!"

            elif c=='i':
                # code for looking up file iteratively
                file_name = raw_input("Enter the name of the file?")
                objID = hashing(file_name)
                destinationHost = root_hostname
                destinationPort = root_port
                data = {
                    'type': 'Lookup File Iteratively',
                    'objID': objID,
                    'client_host': host,
                    'client_port': port
                }
                jsonData = json.dumps(data)
                sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sendSocket.connect((destinationHost, destinationPort))
                sendSocket.sendall(jsonData)
                sendSocket.close()

                print "File name sent to the root"

                c, addr = recvSocket.accept()
                data = c.recv(2048)
                info=json.loads(data)
                flag=0
                while info['type'] != 'Iterative Lookup Return':
                    destinationHost = info['sucHost']
                    destinationPort = info['sucPort']
                    data = {
                        'type': 'Lookup File Iteratively in peer',
                        'objID': objID,
                        'client_host': host,
                        'client_port': port
                    }
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((destinationHost, destinationPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    print "The details sent to successor node!"
                    c, addr = recvSocket.accept()
                    data = c.recv(2048)
                    info = json.loads(data)
                    if(info['type'] == 'File Not Found'):
                        print "The entered file is not found in the system!Please try again!"
                        flag=1
                        break;



                if flag!=1:
                    fileNodeHost = info['FileNodeHost']
                    fileNodePort = info['FileNodePort']

                    data = {
                        'type' : 'Iterative File Request',
                        'objID': objID,
                        'filename':file_name,
                        'client_host' : host,
                        'client_port' : port
                    }
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((destinationHost, destinationPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    c.close()

                    c, addr = recvSocket.accept()
                    data = c.recv(2048)
                    if data:
                        with open(file_name, "wb") as fr:
                            fr.write(data)
                        print "File received successfully and stored!"

            elif c=='e':
                exit(0)

if __name__ == '__main__':
    main()