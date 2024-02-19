import socket
import sys
import random
import json
import threading
import csv

# Global variables to hold key data shared across functions
tuples = []      # To store information about peers in the DHT
year = None      # The year of the dataset to be processed
identifier = None    # The identifier of this peer within the DHT
n = None      # The total number of peers in the DHT
manager_address = None     # The address of the manager node

"""
Handles commands input by the user to interact with the manager.
param clientSocket: The socket for communicating with the manager.
param server_address: The address of the manager server.
"""
def handle_manager_input(clientSocket, server_address):

    global n, year, identifier, tuples, manager_address
    manager_address = server_address

    #runs in infinite loop to send commands to manager
    while True:
        message = input()
        parts = message.split()   #splits the command that is sent 
        clientSocket.sendto(message.encode(), server_address)

        #if the command sent to manager was a register command
        if parts[0] == "register":
            #wait for response from manager
            data,_ = clientSocket.recvfrom(4096)
            print(data.decode('utf-8'))
        #if the command sent to manager was a setup-dht command
        elif parts[0] == "setup-dht":
            #wait for response from manager
            data,_ = clientSocket.recvfrom(4096)
            print(data.decode('utf-8'))

            #if manager sends back SUCCESS message
            if data.decode('utf-8') == 'SUCCESS':
                n = int(parts[2])
                year = int(parts[3])

                #expects n messages from manager, each is a tuple representing a peer in the dht
                for i in range(n):
                    data, _ = clientSocket.recvfrom(4096)
                    peer_info = data.decode('utf-8')
                    dataTuple = json.loads(peer_info)
                    tuples.append(dataTuple)

                

                #nextPeer ['name']['ipv4_address']['p_port']
                #leader will send the setup-dht message so its identifier is 0
                identifier = 0
                nextPeer = tuples[(identifier+1) % n]
                nextPeerAddress = (nextPeer[1], nextPeer[2])

                
                #set-id <identifier> <n=ring size> <tuples>
                #dict to represent data being transported
                message_data = {
                    "command": "set-id",
                    "identifier": identifier+1,
                    "n": n,
                    "tuples": tuples
                }
                #converts into json-formatted string
                message_json = json.dumps(message_data)
                #encodes this string into bytes
                message_bytes = message_json.encode('utf-8')
                #sends these bytes to its neighbor or (identifier+1)%n
                peerSocket.sendto(message_bytes, nextPeerAddress)
    

        
"""
    Handles messages received from other peers in the DHT.
    param peerSocket: The socket for peer-to-peer communication.
"""
def handle_peer_socket(peerSocket):
    global identifier, n, tuples
    #dict to store locally hashed records
    local_hash = {}
    

    while True:
        #waits for peer communication
        data, address = peerSocket.recvfrom(4096)
        
        #data is sent in json, then checks what command was sent
        message_data = json.loads(data)
        command = message_data["command"]

        if command == "set-id":
            identifier = message_data["identifier"]
            
            n = message_data["n"]
            tuples = message_data["tuples"]

            #means its looped back around to the leader and ring in complete
            if(identifier == 0):
                #records the number of records stored at each peer
                record_counter = {peer_id: 0 for peer_id in range(n)}

                #do hash functions
                file_name = '1950-1952/details-1950.csv'
                s = 449
                #get event_id
                with open(file_name, mode='r') as csvfile:
                    csvreader = csv.DictReader(csvfile)
                    for row in csvreader:
                        event_id = int(row["EVENT_ID"])
                        event_string = json.dumps(row)  

                        #pos = event_id mod s
                        pos = event_id % s

                        #id = pos mod n
                        peer_id = pos % n
                        
                        #add to counter for num records at each peer
                        record_counter[peer_id] += 1

                        #record is at the correct peer
                        if(peer_id == identifier):
                            local_hash[pos] = event_string
                        #record needs to be forwarded to the correct peer through the ring
                        else:

                            message_data = {
                                "command": "store",
                                "peer_identifier": peer_id,
                                "event_string": event_string,
                                "pos": pos
                            }
                            message_json = json.dumps(message_data)
                            message_bytes = message_json.encode('utf-8')

                            #get peers neighbor
                            next_identifier = (identifier+1) % n
                            nextPeer = tuples[next_identifier]
                            nextPeerAddress = (nextPeer[1], nextPeer[2])

                            peerSocket.sendto(message_bytes, nextPeerAddress)

                #print record count for each peer
                for peer_id, count in record_counter.items():
                    print(f"Peer ID: {peer_id}, Record Count: {count}")

                #sends message with the leaders name
                message = f"dht-complete {tuples[0][0]}"
                clientSocket.sendto(message.encode(), manager_address)
                #expects response from the manager
                data,_ = clientSocket.recvfrom(4096)
                data = data.decode('utf-8')
                print(data)

            #send set-id to its right neighbor
            else:
                
                next_identifier = (identifier+1) % n
                nextPeer = tuples[next_identifier]
                nextPeerAddress = (nextPeer[1], nextPeer[2])
                message_data = {
                    "command": "set-id",
                    "identifier": next_identifier,
                    "n": n,
                    "tuples": tuples
                }
                message_json = json.dumps(message_data)
                message_bytes = message_json.encode('utf-8')
                peerSocket.sendto(message_bytes, nextPeerAddress)

        elif command == "store":
            # store <peer identifier> <event_string> <pos>
            peer_identifier = message_data['peer_identifier']
            event_string = message_data['event_string']
            pos = message_data['pos']
            #send data to neighbor unless this is the right peer
            if identifier == peer_identifier:
                #store in local hash
                local_hash[pos] = event_string
                
            else:
                #send to neighbor
                nextPeer = tuples[(identifier+1) % n]
                nextPeerAddress = (nextPeer[1], nextPeer[2])
                message_data = {
                    "command": "store",
                    "peer_identifier": peer_identifier,
                    "event_string": event_string,
                    "pos": pos
                }
                message_json = json.dumps(message_data)
                message_bytes = message_json.encode('utf-8')
                peerSocket.sendto(message_bytes, nextPeerAddress)



if __name__ == "__main__":
    #takes in arguments from command line
    #gives servers ip and port number
    serverIP = sys.argv[1]
    serverPort = int(sys.argv[2])
    server_address = (serverIP, int(serverPort))

    #randomly selects ports for communications to avoid conflicts
    managerPort = random.randint(7001, 7499)
    peerPort = random.randint(7001, 7499)
    while managerPort == peerPort:
        peerPort = random.randint(7001, 7499)

    #clientSocket is for communication with manager
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    clientSocket.bind(('', managerPort))
    assigned_ip1, assigned_port1 = clientSocket.getsockname()

    #peerSocket is for communication among peers
    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    peerSocket.bind(('', peerPort))
    assigned_ip2, assigned_port2 = peerSocket.getsockname()

    print(f"Manager socket bound to port {managerPort}, Peer socket bound to port: {peerPort}")

    hostname = socket.gethostname()
    IP = socket.gethostbyname(hostname)
    print(f"running on ip: {IP}")

    #creating threads for handling manager input and peer communication
    manager_thread = threading.Thread(target=handle_manager_input, args=(clientSocket, server_address))
    peer_thread = threading.Thread(target=handle_peer_socket, args=(peerSocket,))

    #starting the threads
    manager_thread.start()
    peer_thread.start()

    manager_thread.join()
    peer_thread.join()
