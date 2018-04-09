#!/usr/bin/env python

import argparse
import socket
import hashlib                  #library for hashing function
import json                     #library for converting data into json format
import sys
import time
import threading

#initializing global variables for using them throughout the program
host = None
port = None
nodeID = None
predecessorHost = None
predecessorPort = None
predecessorNodeID = None
successorHost = None
successorPort = None
successorNodeID = None
nsuccessorHost = None
nsuccessorPort = None
nsuccessorNodeID = None
isRoot = None

#hashing function for finding the node ID
def hashing(ipaddress,port):
    s = hashlib.sha1(ipaddress+str(port))       #using SHA1 hashing
    iphex = s.hexdigest()
    identifier = int(iphex, 16)
    m = 10
    limit  = pow(2,m)                           #limiting the range of node IDs to 1024 for the purpose of this project
    nodeID = identifier%limit                   #can be increased by increasing the value of m
    return nodeID                               #returns node ID

def main():
    parser = argparse.ArgumentParser(add_help=False)        #command line arguments
    parser.add_argument("-m", "--type")
    parser.add_argument("-p", "--own_port")
    parser.add_argument("-h", "--own_hostname")
    parser.add_argument("-r", "--root_port")
    parser.add_argument("-R", "--root_hostname")

    args = parser.parse_args()
    global isRoot
    isRoot  = int(args.type)
    global host                                                     #declaring global variables
    global port
    global nodeID
    global predecessorHost
    global predecessorPort
    global predecessorNodeID
    global successorHost
    global successorPort
    global successorNodeID
    global nsuccessorNodeID
    global nsuccessorHost
    global nsuccessorPort
    host = socket.gethostbyname(args.own_hostname)
    port = int(args.own_port)
    nodeID = hashing(host,port)                                         #calculating node ID
    successorHost = host
    successorPort = port
    print "The node ID of the node is ", nodeID
    recvSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)      #socket connection
    recvSocket.bind((host, port))
    recvSocket.listen(10)                                               #the node listens to incoming connections
    print "Initiaizing file List"                                       #initializing the list of files already stored in the node
    try:
        with open("fileList.txt","r") as fr:
            fileList = json.loads(fr.read())
            print fileList
    except:                                                             #code when the list is empty initially
        print "file list is currently empty"
        fileList ={}

        # the node updates the successor and predecessor everytime an update arrives
        # as multiple functions update predecessors and successors, they can be printed multiple times
        # the last successor and predecessor to be printed are the final predecessor and sucessor

    if isRoot == 1:
        print "Root is running...."
        #these are the functionalities which the peer will carry out when its the root
        while True:
            c, addr = recvSocket.accept()
            data = c.recv(2048)
            if data :
                info = json.loads(data)
                #code to be executed when the node receives data from a new node
                if info['type'] == 'From New Node':
                    print "Data from new node received!!"
                    if successorNodeID == None:
                        successorHost = info['nodeHost']
                        successorPort = info['nodePort']
                        successorNodeID = info['nodeID']
                        predecessorHost = info['nodeHost']
                        predecessorPort = info['nodePort']
                        predecessorNodeID = info['nodeID']
                        nsuccessorHost = host
                        nsuccessorPort = port
                        nsuccessorNodeID = nodeID
                        #sending the data to the new node
                        data = {
                            'type' : 'To New Node',
                            'DestinationNodeID' : successorNodeID,
                            'sucPort' : port,
                            'sucHost' : host,
                            'sucNID' : nodeID,
                            'prePort' : port,
                            'preHost': host,
                            'preNID' : nodeID,
                            'nsucPort': info['nodePort'],
                            'nsucHost': info['nodeHost'],
                            'nsucNID': info['nodeID']
                         }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost,successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    elif info['nodeID']>=nodeID and info['nodeID']<=successorNodeID:
                        #the new node is between the root node and the successor node
                        #send update instructions to the successor node
                        data = {
                            'type' : 'Update Predecessor Node',
                            'DestinationNodeID' : successorNodeID,
                            'preNID' : info['nodeID'],
                            'preHost' : info['nodeHost'],
                            'prePort' : info ['nodePort']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        #send the info for predecessor node and successor node to the new node
                        data = {
                            'type' : 'To New Node',
                            'DestinationNodeID' : info['nodeID'],
                            'sucPort' : successorPort,
                            'sucHost' : successorHost,
                            'sucNID' : successorNodeID,
                            'prePort' : port,
                            'preHost': host,
                            'preNID' : nodeID,
                            'nsucPort' :nsuccessorPort,
                            'nsucHost' :nsuccessorHost,
                            'nsucNID' :nsuccessorNodeID
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['nodeHost'], info['nodePort']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        #update previous node's next successor node
                        data = {
                            'type' : 'Update Next Successor Node',
                            'DestinationNodeID' : predecessorNodeID,
                            'nsucHost' : info['nodeHost'],
                            'nsucPort' : info['nodePort'],
                            'nsucNID'  : info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # update the root node's successor node
                        nsuccessorNodeID = successorNodeID
                        nsuccessorHost = successorHost
                        nsuccessorPort = successorPort
                        successorNodeID = info['nodeID']
                        successorHost = info['nodeHost']
                        successorPort = info['nodePort']
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    elif info['nodeID']>=nodeID and nodeID>=successorNodeID:
                        #the new node is between the root node and the successor node
                        #send update instructions to the successor node
                        data = {
                            'type' : 'Update Previous Node',
                            'DestinationNodeID' : successorNodeID,
                            'preNID' : info['nodeID'],
                            'preHost' : info['nodeHost'],
                            'prePort' : info ['nodePort']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        #send the info for predecessor node and successor node to the new node
                        data = {
                            'type' : 'To New Node',
                            'DestinationNodeID' : info['nodeID'],
                            'sucPort' : successorPort,
                            'sucHost' : successorHost,
                            'sucNID' : successorNodeID,
                            'prePort' : port,
                            'preHost': host,
                            'preNID' : nodeID,
                            'nsucPort': nsuccessorPort,
                            'nsucHost': nsuccessorHost,
                            'nsucNID': nsuccessorNodeID
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['nodeHost'], info['nodePort']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # update previous node's next successor node
                        data = {
                            'type': 'Update Next Successor Node',
                            'DestinationNodeID': predecessorNodeID,
                            'nsucHost': info['nodeHost'],
                            'nsucPort': info['nodePort'],
                            'nsucNID': info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # update the root node's successor node
                        nsuccessorNodeID = successorNodeID
                        nsuccessorHost = successorHost
                        nsuccessorPort = successorPort
                        successorNodeID = info['nodeID']
                        successorPort = info['nodePort']
                        successorHost = info['nodeHost']
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    elif info['nodeID'] <= successorNodeID and nodeID >= successorNodeID:
                        print "New Node is between root node and successor node"
                        # the new node is between the root node and the successor node
                        # send update instructions to the successor node
                        data = {
                            'type': 'Update Previous Node',
                            'DestinationNodeID': successorNodeID,
                            'preNID': info['nodeID'],
                            'preHost': info['nodeHost'],
                            'prePort': info['nodePort']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # send the info for predecessor node and successor node to the new node
                        data = {
                            'type': 'To New Node',
                            'DestinationNodeID': info['nodeID'],
                            'sucPort': successorPort,
                            'sucHost': successorHost,
                            'sucNID': successorNodeID,
                            'prePort': port,
                            'preHost': host,
                            'preNID': nodeID,
                            'nsucPort': nsuccessorPort,
                            'nsucHost': nsuccessorHost,
                            'nsucNID': nsuccessorNodeID
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['nodeHost'], info['nodePort']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # update previous node's next successor node
                        data = {
                            'type': 'Update Next Successor Node',
                            'DestinationNodeID': predecessorNodeID,
                            'nsucHost': info['nodeHost'],
                            'nsucPort': info['nodePort'],
                            'nsucNID': info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # update the root node's successor node
                        nsuccessorNodeID = successorNodeID
                        nsuccessorHost = successorHost
                        nsuccessorPort = successorPort
                        successorNodeID = info['nodeID']
                        successorPort = info['nodePort']
                        successorHost = info['nodeHost']
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    else:

                        #the data for the new node is sent to the successor nodes to find out its position
                        data = {
                            'type': 'From New Node',
                            'nodeID': info['nodeID'],
                            'nodeHost': info['nodeHost'],
                            'nodePort': info['nodePort'],
                            'rootNID' : nodeID
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost,successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                elif(info['type'] == 'To New Node'):
                    #send information for update of predecessor and successor nodes to the new node
                    data = {
                        'type' : 'To New Node',
                        'DestinationNodeID' : info['newNodeID'],
                        'sucPort': info['sucPort'],
                        'sucHost': info['sucHost'],
                        'sucNID': info['sucNID'],
                        'prePort': info['prePort'],
                        'preHost': info['preHost'],
                        'preNID': info['preNID'],
                        'nsucHost' : info['nsucHost'],
                        'nsucPort' : info['nsucPort'],
                        'nsucNID' : info['nsucNID']
                    }
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((info['newNodeHost'], info['newNodePort']))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()

                    #send updates instructions to the successor node
                    data = {
                        'type': 'Update Predecessor Node',
                        'DestinationNodeID': info['sucNID'],
                        'prePort': info['newNodePort'],
                        'preHost': info['newNodeHost'],
                        'preNID': info['newNodeID']
                    }
                    #print "send updates to the successor node of the new node about the predecessor node"
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((successorHost, successorPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()


                    #send updates instructions to the predecessor node
                    data = {
                        'type': 'Update Successor Node',
                        'DestinationNodeID': info['preNID'],
                        'sucPort': info['newNodePort'],
                        'sucHost': info['newNodeHost'],
                        'sucNID': info['newNodeID']
                    }
                    #print "send updates to the predecessor node of the new node about the successor"
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((successorHost, successorPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()

                elif (info['type'] == 'Update Predecessor Node'):
                    #update instructions to update the precessor node of the current node
                    if (info['DestinationNodeID'] == nodeID): #if the update instructions are for this node, excecute the necessary code
                        predecessorNodeID = info['preNID']
                        predecessorPort = info['prePort']
                        predecessorHost = info['preHost']

                        print "Updated the previous host!"
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                        # code for load balancing in case any file stored on the current node belongs the new node
                        tmpList = {}
                        for obj in fileList:
                         if obj <= predecessorNodeID:                   #checks if any object stored on this node is less than the new node ID
                             print "balancing the file load"
                             file_name = fileList[obj]
                             with open(file_name, "rb") as fw:          #send the file to the appropriate node
                                 data = {
                                     'type': 'Store File Here',
                                     'objID': obj,
                                     'filename': fileList[obj],
                                     'data': fw.read()
                                 }
                                 jsonData = json.dumps(data)
                                 sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                 sendSocket.connect((predecessorHost, predecessorPort))
                                 sendSocket.sendall(jsonData)
                                 sendSocket.close()
                         else:

                                tmpList[obj] = fileList[obj]
                        fileList = tmpList                              #update the list of files on this node
                        with open("fileList.txt","w") as fw:
                            jsonfileList = json.dumps(fileList)
                            fw.write(jsonfileList)


                    else:                               #else send the instructions to the successor node
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                # update instructions to update the successor node of the current node
                elif (info['type'] == 'Update Successor Node'):
                    if (info['DestinationNodeID'] == nodeID):       #if the instructions are for this node, execute the necessary code
                        successorNodeID = info['sucNID']
                        successorPort = info['sucPort']
                        successorHost = info['sucHost']

                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID
                    else:
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                # update instructions to update the next successor node of the current node
                elif (info['type'] == 'Update Next Successor Node'):
                    if (info['DestinationNodeID'] == nodeID):
                        nsuccessorHost = info['nsucHost']
                        nsuccessorPort = info['nsucPort']
                        nsuccessorNodeID = info['nsucNID']

                        print "The succesor is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    else:
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                #update instructions for the updating the predecessor node in case of a node crash
                elif (info['type'] == 'Update Predecessor Node after fixing'):
                    predecessorHost = info['preHost']
                    predecessorPort = info['prePort']
                    predecessorNodeID = info['preNID']

                    data = {
                        'type': 'Update next Successor after fixing',
                        'nsucHost': successorHost,
                        'nsucPort': successorPort,
                        'nsucNID': successorNodeID
                    }
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((predecessorHost, predecessorPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    #print "ring is fixed!"
                    print "The succesor is ", successorNodeID
                    print "The predecessor is ", predecessorNodeID

                # update instructions for the updating the successor node in case of a node crash
                elif (info['type'] == 'Update next Successor after fixing'):
                    nsuccessorHost = info['nsucHost']
                    nsuccessorPort = info['nsucPort']
                    nsuccessorNodeID = info['nsucNID']

                    #print "Ring successfully fixed!"
                    print "The succesor is ", successorNodeID
                    print "The predecessor is ", predecessorNodeID

                #operations for client data


                elif (info['type'] == 'file node identification'):
                    #code for searching the suitable position for the storing the file
                    print "received data for file node identification"
                    objID = info['objID']               #object ID sent by the client
                    if(objID<=nodeID and objID>=predecessorNodeID): #if the file belongs to this(root) node,
                        data = {                                    #send the acknowledgemnt to the client
                            'type' : 'file node ack',
                            'objID' : objID,
                            'destinationHost' : host,
                            'destinationPort' : port
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending data to client"

                    elif(objID<=nodeID and nodeID<=predecessorNodeID):
                        data = {
                            'type': 'file node ack',
                            'objID': objID,
                            'destinationHost': host,
                            'destinationPort': port
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending data to client"

                    elif objID>=predecessorNodeID and predecessorNodeID>=nodeID:
                        data = {
                            'type': 'file node ack',
                            'objID': objID,
                            'destinationHost': host,
                            'destinationPort': port
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending data to client"

                    else:
                        jsonData=json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending data for file node to next node"

                #code for sending the details of the desired node received from the successor node to the client
                elif(info['type'] == 'file node ack'):
                    jsonData = json.dumps(info)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((info['client_host'], info['client_port']))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    "sending data to client after receiving data of file from successor node"

                #code for storing the file in the node
                elif info['type'] == 'file storage':
                    #filename = info['filename'] + 'copy'
                    with open(info['filename'],'wb') as fw:
                        fw.write(info['data'])
                        fileList[info['objID']] = info['filename']
                        print "The final list of files is ", fileList           #updating the fileList in case of storing a new file
                        with open("fileList.txt","w") as fw:
                            jsonfileList = json.dumps(fileList)
                            fw.write(jsonfileList)
                        print "File successully stored on the node!"

                #code for Recursive file look up request
                elif info['type'] == 'Lookup file Recursively':
                    print "Recursive file look up request!"
                    if info['objID'] in fileList:               #if the file is in the current node, return the details of the node to the client
                        data = {
                            'type' : 'Recursive Lookup Return',
                            'FileNodeHost' : host,
                            'FileNodePort' : port
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "Sending node details to the client"
                    else:
                        data = {
                            'type' : 'Lookup file Recursively in peer',             #if the file is not in this node, send the request to successive nodes
                            'objID' : info['objID'],
                            'client_host' : info['client_host'],
                            'client_port' : info['client_port']
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "Sending file details to the next node!"

                elif info['type'] == 'Lookup file Recursively in peer':
                        data = {
                            'type' :'File Not Found'                                #if the recursive look up request returns to the
                        }                                                           #root after traversing the ring, it concludes that the ring does not have the file
                        jsonData = json.dumps(data)                                 #send updates to the client
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                elif info['type'] == 'Recursive Lookup Return':                     #if the file is found after the recursive file look up, send the node details to the client
                    data = {
                        'type': 'Recursive Lookup Return',
                        'FileNodeHost': info['FileNodeHost'],
                        'FileNodePort': info['FileNodePort']
                    }
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((info['client_host'], info['client_port']))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    print "Sending node details to the client!"

                elif info['type'] == 'Recursive File Request':      #code to send the file to the client
                    objID = info['objID']
                    file_name = fileList[objID]

                    with open(file_name, "rb") as fr:
                        data = fr.read()
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "File sent successfully!"

                #code to check for the request of Iterative look up
                elif info['type'] == 'Lookup File Iteratively':
                    print "request for iterative lookup received"
                    objID = info['objID']                       #if the file is in the current node, send the node details to the client
                    if objID in fileList:
                        data = {
                            'type': 'Iterative Lookup Return',
                            'FileNodeHost': host,
                            'FileNodePort': port,
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "File found! sending data to the client!"

                    else:                                      #if the file is not found, send the update to the client
                        data = {
                            'type': 'No File Found',
                            'sucHost': successorHost,
                            'sucPort': successorPort
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "File not found!"

                elif info['type'] == 'Lookup File Iteratively in peer':     #if the iterative look up returns to the root after traversing the ring,
                    data = {                                                #it concludes that the file is not in the ring,sends message to the client
                        'type' : 'File Not Found'
                    }
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((info['client_host'], info['client_port']))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()


                elif info['type'] == 'Iterative File Request':
                    objID = info['objID']
                    file_name = fileList[objID]

                    with open(file_name, "rb") as fr:
                        data = fr.read()
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "File sent successfully!"

                elif info['type'] == 'Store File Here':     #code to store the file in case of load balancing
                    objID = info['objID']
                    file_name = info['filename']
                    with open(info['filename'],"wb") as fr:
                        fr.write(info['data'])
                        print "File transfered successfully!"

                #time.sleep(1)





    elif(isRoot==0):
        rootHost = socket.gethostbyname(args.root_hostname)
        rootPort = int(args.root_port)
        print rootHost
        initialData = {
            'type' : 'From New Node',
            'nodeID' : nodeID,
            'nodeHost' : host,
            'nodePort' : port
        }
        jsoninitialData = json.dumps(initialData)
        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sendSocket.connect((rootHost,rootPort))
        sendSocket.sendall(jsoninitialData)
        sendSocket.close()
        print "Data sent to root node!!"
        while True:
            time.sleep(1)
            c, addr = recvSocket.accept()
            data = c.recv(2048)
            if data:
                info = json.loads(data)

                if (info['type'] == 'From New Node'):

                    if info['nodeID'] >= nodeID and info['nodeID'] <= successorNodeID:
                        # send the info for predecessor node and successor node to the new node
                        data = {
                            'type': 'To New Node',
                            'DestinationNodeID': info['rootNID'],
                            'sucPort': successorPort,
                            'sucHost': successorHost,
                            'sucNID': successorNodeID,
                            'prePort': port,
                            'preHost': host,
                            'preNID': nodeID,
                            'nsucHost' : nsuccessorHost,
                            'nsucPort' : nsuccessorPort,
                            'nsucNID' : nsuccessorNodeID,
                            'newNodeHost' : info['nodeHost'],
                            'newNodePort' : info['nodePort'],
                            'newNodeID' : info['nodeID']

                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        #send instructions to update to the predecessor node about the next successor
                        data = {
                            'type' : 'Update Next Successor Node',
                            'DestinationNodeID' : predecessorNodeID,
                            'nsucHost' : info['nodeHost'],
                            'nsucPort' : info['nodePort'],
                            'nsucNID' : info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # update the this node's successor node
                        nsuccessorHost = successorHost
                        nsuccessorPort = successorPort
                        nsuccessorNodeID = successorNodeID
                        successorNodeID = info['nodeID']
                        successorPort = info['nodePort']
                        successorHost = info['nodeHost']
                        #print "The info for the succesor node is updated!"
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID



                    elif info['nodeID'] >= nodeID and nodeID >= successorNodeID:
                        # send the info for predecessor node and successor node to the new node
                        data = {
                            'type': 'To New Node',
                            'DestinationNodeID': info['rootNID'],
                            'sucPort': successorPort,
                            'sucHost': successorHost,
                            'sucNID': successorNodeID,
                            'prePort': port,
                            'preHost': host,
                            'preNID': nodeID,
                            'nsucHost': nsuccessorHost,
                            'nsucPort': nsuccessorPort,
                            'nsucNID': nsuccessorNodeID,
                            'newNodeHost': info['nodeHost'],
                            'newNodePort': info['nodePort'],
                            'newNodeID': info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # send instructions to update to the predecessor node about the next successor
                        data = {
                            'type': 'Update Next Successor Node',
                            'DestinationNodeID': predecessorNodeID,
                            'nsucHost': info['nodeHost'],
                            'nsucPort': info['nodePort'],
                            'nsucNID': info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # update the root node's successor node
                        nsuccessorHost = successorHost
                        nsuccessorPort = successorPort
                        nsuccessorNodeID = successorNodeID
                        successorNodeID = info['nodeID']
                        successorPort = info['nodePort']
                        successorHost = info['nodeHost']
                        #print "The info for the succesor node is updated!"
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    elif info['nodeID'] <= successorNodeID and nodeID >= successorNodeID:
                        # send the info for predecessor node and successor node to the new node
                        data = {
                            'type': 'To New Node',
                            'DestinationNodeID': info['rootNID'],
                            'sucPort': successorPort,
                            'sucHost': successorHost,
                            'sucNID': successorNodeID,
                            'prePort': port,
                            'preHost': host,
                            'preNID': nodeID,
                            'nsucHost': nsuccessorHost,
                            'nsucPort': nsuccessorPort,
                            'nsucNID': nsuccessorNodeID,
                            'newNodeHost': info['nodeHost'],
                            'newNodePort': info['nodePort'],
                            'newNodeID': info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # send instructions to update to the predecessor node about the next successor
                        data = {
                            'type': 'Update Next Successor Node',
                            'DestinationNodeID': predecessorNodeID,
                            'nsucHost': info['nodeHost'],
                            'nsucPort': info['nodePort'],
                            'nsucNID': info['nodeID']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()


                        # update the root node's successor node
                        nsuccessorHost = successorHost
                        nsuccessorPort = successorPort
                        nsuccessorNodeID = successorNodeID
                        successorNodeID = info['nodeID']
                        successorPort = info['nodePort']
                        successorHost = info['nodeHost']
                        #print "The info for the succesor node is updated!"
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    else:
                        #send the data from new node to the next node to find out the credentials
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()



                elif(info['type'] == 'To New Node'):

                    if(info['DestinationNodeID'] == nodeID):
                        successorHost = info['sucHost']
                        successorPort = info['sucPort']
                        successorNodeID = info['sucNID']
                        predecessorHost = info['preHost']
                        predecessorPort = info['prePort']
                        predecessorNodeID = info['preNID']
                        nsuccessorPort = info['nsucPort']
                        nsuccessorHost = info['nsucHost']
                        nsuccessorNodeID = info['nsucNID']

                        #print "The information for successor and predecessor node updated!"
                        #print "This node is ", nodeID
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    else:
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()


                elif(info['type'] == 'Update Predecessor Node'):
                    if(info['DestinationNodeID'] == nodeID):
                        predecessorNodeID = info['preNID']
                        predecessorPort = info['prePort']
                        predecessorHost = info['preHost']

                        #print "Updated the predecessor node!"


                        #code for load balancing
                        tmpList = {}
                        for obj in fileList:
                            if obj<=predecessorNodeID:
                                print "balancing the load"
                                with open(fileList[obj],"rb") as fw:
                                    data = {
                                        'type' : 'Store File Here',
                                        'objID' : obj,
                                        'filename' : fileList[obj],
                                        'data' : fw.read()
                                    }
                                    jsonData = json.dumps(data)
                                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    sendSocket.connect((predecessorHost, predecessorPort))
                                    sendSocket.sendall(jsonData)
                                    sendSocket.close()
                                    print "File transferred successfuly"
                            else:

                                tmpList[obj] = fileList[obj]
                        fileList = tmpList
                        with open("fileList.txt","w") as fw:
                            jsonfileList = json.dumps(fileList)
                            fw.write(jsonfileList)
                        #print "The final list of objects stored are \n",fileList
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID


                    else:
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                elif (info['type'] == 'Update Successor Node'):
                    if (info['DestinationNodeID'] == nodeID):
                        successorNodeID = info['sucNID']
                        successorPort = info['sucPort']
                        successorHost = info['sucHost']

                        #print "Updated the successor node!"
                        print "The successor node is ", successorNodeID
                        print "The predecessor is ", predecessorNodeID
                    else:
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()


                elif (info['type'] == 'Update Next Successor Node'):
                    if(info['DestinationNodeID']==nodeID):
                        nsuccessorHost = info['nsucHost']
                        nsuccessorPort = info['nsucPort']
                        nsuccessorNodeID = info['nsucNID']

                        #print "Updated the next successor!"
                        print "The succesor is ",successorNodeID
                        print "The predecessor is ", predecessorNodeID

                    else:
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()


                elif(info['type'] == 'Update Predecessor Node after fixing'):
                    predecessorHost=info['preHost']
                    predecessorPort=info['prePort']
                    predecessorNodeID=info['preNID']

                    data ={
                        'type' : 'Update next Successor after fixing',
                        'nsucHost' : successorHost,
                        'nsucPort' : successorPort,
                        'nsucNID' : successorNodeID
                    }
                    jsonData = json.dumps(data)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((predecessorHost, predecessorPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    #print "ring is fixed!"
                    print "The succesor is ", successorNodeID
                    print "The predecessor is ", predecessorNodeID


                elif(info['type'] == 'Update next Successor after fixing'):
                    nsuccessorHost=info['nsucHost']
                    nsuccessorPort=info['nsucPort']
                    nsuccessorNodeID=info['nsucNID']

                    #print "Ring successfully fixed!"
                    print "The succesor is ", successorNodeID
                    print "The predecessor is ", predecessorNodeID

                #code for client operations
                elif (info['type'] == 'file node identification'):
                    print "received data for file node identification"
                    objID = info['objID']
                    if(objID<=nodeID and objID>=predecessorNodeID):
                        data = {
                            'type' : 'file node ack',
                            'objID' : objID,
                            'destinationHost' : host,
                            'destinationPort' : port,
                            'client_host' : info['client_host'],
                            'client_port' : info['client_port']
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending data of file node to previous node"

                    elif(objID<=nodeID and nodeID<=predecessorNodeID):
                        data = {
                            'type': 'file node ack',
                            'objID': objID,
                            'destinationHost': host,
                            'destinationPort': port,
                            'client_host': info['client_host'],
                            'client_port': info['client_port']
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending data of file node to previous node"

                    elif objID>=predecessorNodeID and nodeID<=predecessorNodeID:
                        data = {
                            'type': 'file node ack',
                            'objID': objID,
                            'destinationHost': host,
                            'destinationPort': port,
                            'client_host': info['client_host'],
                            'client_port': info['client_port']
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending data of file node to previous node"

                    else:
                        jsonData=json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "sending file node identification data to next node "

                elif(info['type'] == 'file node ack'):
                    jsonData = json.dumps(info)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((predecessorHost, predecessorPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    print "sending data of file node to previous node"

                elif info['type'] == 'file storage':
                    with open(info['filename'],'wb') as fw:
                        fw.write(info['data'])
                        fileList[info['objID']] = info['filename']
                        with open("fileList.txt","w") as fw:
                            jsonfileList = json.dumps(fileList)
                            fw.write(jsonfileList)
                        print "File successully stored on the node!"
                        print fileList

                elif info['type'] == 'Lookup file Recursively in peer':
                    print "Recursive file lookup request!"
                    if info['objID'] in fileList:
                        data = {
                            'type' : 'Recursive Lookup Return',
                            'FileNodeHost' : host,
                            'FileNodePort' : port,
                            'client_host' : info['client_host'],
                            'client_port' : info['client_port']
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "Node found! Sending the node details to the root!"
                    else:
                        jsonData = json.dumps(info)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "Sending file details to other node!"

                elif info['type'] == 'Recursive Lookup Return':
                    jsonData = json.dumps(info)
                    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendSocket.connect((predecessorHost, predecessorPort))
                    sendSocket.sendall(jsonData)
                    sendSocket.close()
                    print "Sending node details to the root!"

                elif info['type'] == 'Recursive File Request':
                    objID = info['objID']
                    file_name = fileList[objID]

                    with open(file_name, "rb") as fr:
                        data = fr.read()
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "File sent successfully!"

                elif info['type'] == 'Lookup File Iteratively in peer':
                    print "Request for Iterative Lookup received"
                    objID = info['objID']
                    if objID in fileList:
                        data = {
                            'type': 'Iterative Lookup Return',
                            'FileNodeHost': host,
                            'FileNodePort': port,
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                    else:
                        data = {
                            'type': 'No File Found',
                            'sucHost': successorHost,
                            'sucPort':successorPort
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "File not found!"


                elif info['type'] == 'Iterative File Request':
                    objID = info['objID']
                    file_name = fileList[objID]

                    with open(file_name, "rb") as fr:
                        data = fr.read()
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((info['client_host'], info['client_port']))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        print "File sent successfully!"

                elif info['type'] == 'Store File Here':
                    objID = info['objID']
                    file_name = info['filename']
                    with open(info['filename'],"wb") as fr:
                        fr.write(info['data'])
                        print "File transfered successfully!"



                #time.sleep(1)


#function to check if any node crashes, Runs in the background
def bgcheck():
    global successorHost
    global successorPort
    global successorNodeID
    global nodeID
    global host
    global port
    global nsuccessorPort
    global nsuccessorHost
    global nsuccessorNodeID
    global predecessorHost
    global predecessorPort
    global predecessorNodeID
    global isRoot

    while True:
        if(isRoot==1):
            if successorNodeID==None:                           #checks if the root is alone initially
                continue
            else:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)       #if not, it checks if the successor node has crashed
                    s.connect((successorHost, successorPort))
                    s.close()
                    time.sleep(5)
                    # print "no node is crashed!"
                except:                                                         #code for fixing the ring in case the successor node crashes
                    print "Successor node crashed! Fixing the ring!"
                    if nsuccessorNodeID==nodeID:
                        successorHost = None
                        successorPort = None
                        successorNodeID = None
                    else:
                        successorHost = nsuccessorHost
                        successorPort = nsuccessorPort
                        successorNodeID = nsuccessorNodeID
                        # send updates to the successor node about the predecessor node
                        data = {
                            'type': 'Update Predecessor Node after fixing',
                            'preHost': host,
                            'prePort': port,
                            'preNID': nodeID
                        }
                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((successorHost, successorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()

                        # send updates to the predecessor node about the updates of successor node
                        data = {
                            'type': 'Update Next Successor Node',
                            'DestinationNodeID': predecessorNodeID,
                            'nsucHost': successorHost,
                            'nsucPort': successorPort,
                            'nsucNID': successorNodeID
                        }

                        jsonData = json.dumps(data)
                        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sendSocket.connect((predecessorHost, predecessorPort))
                        sendSocket.sendall(jsonData)
                        sendSocket.close()
                        time.sleep(2)

        else:                               #code for checking successor nodes of a normal peer

            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((successorHost,successorPort))
                s.close()
                time.sleep(5)
                #print "no node is crashed!"
            except:
                print "Successor node crashed! Fixing the ring!"
                successorHost=nsuccessorHost
                successorPort=nsuccessorPort
                successorNodeID=nsuccessorNodeID
                #send updates to the successor node about the predecessor node
                data={
                    'type' : 'Update Predecessor Node after fixing',
                    'preHost' : host,
                    'prePort' : port,
                    'preNID' :nodeID
                }
                jsonData = json.dumps(data)
                sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sendSocket.connect((successorHost, successorPort))
                sendSocket.sendall(jsonData)
                sendSocket.close()

                #send updates to the predecessor node about the updates of successor node
                data = {
                    'type': 'Update Next Successor Node',
                    'DestinationNodeID' : predecessorNodeID,
                    'nsucHost': successorHost,
                    'nsucPort': successorPort,
                    'nsucNID': successorNodeID
                }

                jsonData = json.dumps(data)
                sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sendSocket.connect((predecessorHost, predecessorPort))
                sendSocket.sendall(jsonData)
                sendSocket.close()
                time.sleep(2)


class Thread1(threading.Thread):
    def run(self):
        main()

class Thread2(threading.Thread):
    def run(self):
        bgcheck()


if __name__ == '__main__':
    t1=Thread1(name = "Main Process")
    t2=Thread2(name = "Background Check")

    t1.start()
    time.sleep(5)
    t2.start()