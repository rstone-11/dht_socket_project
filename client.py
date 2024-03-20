import socket
import sys
import random
import json
import threading
import csv
import math
import time

# Global variables to hold key data shared across functions
tuples = []      # To store information about peers in the DHT
year = None      # The year of the dataset to be processed
identifier = None    # The identifier of this peer within the DHT
n = None      # The total number of peers in the DHT
manager_address = None     # The address of the manager node
s = None      #used in the hash function
leaving_peer = None

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
            tuples.clear()
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
                    "tuples": tuples,
                    "year": year,
                    "reset": 'no'
                }
                #converts into json-formatted string
                message_json = json.dumps(message_data)
                #encodes this string into bytes
                message_bytes = message_json.encode('utf-8')
                #sends these bytes to its neighbor or (identifier+1)%n
                peerSocket.sendto(message_bytes, nextPeerAddress)

        elif parts[0] == "query-dht":
            #expect 3-tuple of random peer - [name, ip address, p-port] or FAILURE
            data, address = clientSocket.recvfrom(4096)
            r_info = data.decode('utf-8')

            if isinstance(r_info, str) and r_info == 'FAILURE':
                print('FAILURE - received failure from manager')
                continue

            r_peer = json.loads(r_info)
            random_address = (r_peer[1], r_peer[2])

            #send a find-event command to this peer 
            #I = list from 0 to n-1
            n = r_peer[5]
            I = list(range(n))
            id_seq = []
            set_of_ids = [5536849, 2402920, 5539287, 55770111]
            random_id = random.choice(set_of_ids)
            print(f'event_id is {random_id}')
            message_data = {
                "command": "find-event",
                "event_id": random_id,
                "s_tuple": (r_peer[3], r_peer[4], peerPort),
                "I": I,
                "id_seq": id_seq
            }
            
            message_json = json.dumps(message_data)
            message_bytes = message_json.encode('utf-8')
            peerSocket.sendto(message_bytes, random_address)

        elif parts[0] == 'deregister':
            #expects either SUCCESS or FAILURE
            data, _ = clientSocket.recvfrom(4096)
            message = data.decode('utf-8')

            if message == 'SUCCESS':
                print("terminating...")
                sys.exit(0)
            else:
                print('failed to deregister')
        
        elif parts[0] == 'teardown-dht':
            data, address = clientSocket.recvfrom(4096)
            message = data.decode('utf-8')

            print(f"{message}")
            if message == "SUCCESS":

                #send teardown command to peer
                next_id = (identifier+1) % n
                next_address = (tuples[next_id][1], tuples[next_id][2])
                message = {
                    "command": "teardown",
                    "id": 0,
                    "leave": "no"
                }
                message_j = json.dumps(message)
                peerSocket.sendto(message_j.encode('utf-8'), next_address)
            else:
                print(f'mesage was {message}')

        elif parts[0] == 'leave-dht':
            #expects SUCCESS or FAILURE from manager
            data, address = clientSocket.recvfrom(4096)
            message = data.decode('utf-8')

            if message == "SUCCESS":
                leaving_peer = tuples[identifier]
                print(f"Received SUCCESS from manager starting teardown")
                next_id = (identifier+1)%n
                next_peer = tuples[identifier]
                
                message = {
                    "command": "teardown",
                    "id": identifier,
                    "leave": 'yes'
                }
                message_j = json.dumps(message)
                peerSocket.sendto(message_j.encode('utf-8'), (next_peer[1], next_peer[2]))

                #expect SUCCESS to begin create of new tuples
                #wait for new tuples to be sent back from manager
                data, s_address = clientSocket.recvfrom(4096)
                data_decode = data.decode('utf-8')

                if data_decode == "SUCCESS":
                    #size of dht is decreased by 1
                    n = n-1
                    tuples.clear()
                    for i in range(n):
                        data, _ = clientSocket.recvfrom(4096)
                        peer_info = data.decode('utf-8')
                        dataTuple = json.loads(peer_info)
                        tuples.append(dataTuple)

                    #leader will send the setup-dht message so its identifier is 0
                    newLeader = tuples[0]
                    #sending to the new leader
                    reset_message = {
                        "command": "reset-id",
                        "leaving-peer": leaving_peer,
                        "new-n": n,
                        "identifier": 0,
                        "tuples": tuples,
                        "function": 'leave'
                    }
                    
                    message_json = json.dumps(reset_message)
                    message_bytes = message_json.encode('utf-8')
                    
                    peerSocket.sendto(message_bytes, (tuples[0][1], tuples[0][2]))

                    print('waiting for dht-rebuilt response from manager')
                    #wait for dht-rebuilt SUCCESS from manager
                    data, _ = clientSocket.recvfrom(4096)
                    message = data.decode('utf-8')
                    print(f'dht-rebuilt response was a {message}')


                else:
                    print(f"Received this from teardown-complete: {data_decode}")
            
            else:
                print(f"{message}")

        elif parts[0] == 'join-dht':
            #expects SUCCESS or FAILURE from manager
            data, address = clientSocket.recvfrom(4096)
            message = data.decode('utf-8')
            parts = message.split()

            if parts[0] == "SUCCESS":
                #receive joining peer tuple and append to tuples
                #set the joining peers identifier
                #send reset-id then rebuild-dht
                n = int(parts[1])

                message = {
                    "command": "teardown",
                    "id": 0,
                    "leave": 'yes'
                }
                message_j = json.dumps(message)
                peerSocket.sendto(message_j.encode('utf-8'), (parts[2], int(parts[3])))

                #expect SUCCESS to begin create of new tuples
                data, s_address = clientSocket.recvfrom(4096)
                data_decode = data.decode('utf-8')
                #size of dht is increased by 1
                n = n+1
                tuples.clear()
                for i in range(n):
                    data, _ = clientSocket.recvfrom(4096)
                    peer_info = data.decode('utf-8')
                    dataTuple = json.loads(peer_info)
                    tuples.append(dataTuple)

                #sending to the leader
                reset_message = {
                    "command": "reset-id",
                    "new-n": n,
                    "identifier": 0,
                    "tuples": tuples,
                    "function": "join"
                }
                
                message_json = json.dumps(reset_message)
                message_bytes = message_json.encode('utf-8')
                
                peerSocket.sendto(message_bytes, (tuples[0][1], tuples[0][2]))

                #wait for dht-rebuilt SUCCESS from manager
                data, _ = clientSocket.recvfrom(4096)
                message = data.decode('utf-8')
                print(f'dht-rebuilt response was a {message}')
            else:
                print(f"{message}")

        
"""
    Handles messages received from other peers in the DHT.
    param peerSocket: The socket for peer-to-peer communication.
"""
def handle_peer_socket(peerSocket):
    global identifier, n, tuples, s, leaving_peer
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
            year = message_data['year']

            print(f'identifier: {identifier}, name: {tuples[identifier][0]}')

            #means its looped back around to the leader and ring in complete
            if(identifier == 0):
                #records the number of records stored at each peer
                record_counter = {peer_id: 0 for peer_id in range(n)}
                
                #do hash functions
                file_name = f"data/details-{year}.csv"

                with open(file_name, mode='r') as csvfile:
                    #list of dictionaries
                    #attributes - EVENT_ID,STATE,YEAR,MONTH_NAME,EVENT_TYPE,CZ_TYPE,CZ_NAME,INJURIES_DIRECT,INJURIES_INDIRECT,DEATHS_DIRECT,DEATHS_INDIRECT,DAMAGE_PROPERTY,DAMAGE_CROPS,TOR_F_SCALE
                    rows = list(csv.DictReader(csvfile))
                    #get length of list = number of rows
                    row_count = len(rows)
                    s = next_prime(2 * row_count)
                    
                    for row in rows:
                        event_id = int(row["EVENT_ID"])
                        #event_string = json.dumps(row)  

                        #pos = event_id mod s
                        pos = event_id % s

                        #id = pos mod n
                        peer_id = pos % n
                        
                        #add to counter for num records at each peer
                        record_counter[peer_id] += 1

                        #record is at the correct peer
                        if peer_id == identifier:
                            if pos not in local_hash:
                                local_hash[pos] = [row]
                            else:
                                local_hash[pos].append(row)
                            
                        #record needs to be forwarded to the correct peer through the ring
                        else:

                            store_message = {
                                "command": "store",
                                "peer_identifier": peer_id,
                                "event_row": row,
                                "pos": pos,
                                "s": s
                            }
                            message_json = json.dumps(store_message)
                            message_bytes = message_json.encode('utf-8')

                            #get peers neighbor
                            next_identifier = (identifier+1) % n
                            nextPeer = tuples[next_identifier]
                            nextPeerAddress = (nextPeer[1], nextPeer[2])

                            peerSocket.sendto(message_bytes, nextPeerAddress)

                #print record count for each peer
                for peer_id, count in record_counter.items():
                    print(f"Peer ID: {peer_id}, Record Count: {count}")

                if message_data['reset'] == 'yes':
                    if leaving_peer is not None:
                        message = f"dht-rebuilt {leaving_peer[0]} {tuples[0][0]}"
                    else: message = f"dht-rebuilt {tuples[len(tuples)-1][0]} {tuples[0][0]}"
                    clientSocket.sendto(message.encode('utf-8'), manager_address)
                    leaving_peer = None
                else:
                    #sends message with the leaders name
                    print('sending dht-complete to manager')
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
                set_message = {
                    "command": "set-id",
                    "identifier": next_identifier,
                    "n": n,
                    "tuples": tuples,
                    "year": year,
                    "reset": message_data['reset'] 
                }
                message_json = json.dumps(set_message)
                message_bytes = message_json.encode('utf-8')
                peerSocket.sendto(message_bytes, nextPeerAddress)

        elif command == "store":
            # store <peer identifier> <event_string> <pos>
            peer_identifier = message_data['peer_identifier']
            event_row = message_data['event_row']
            pos = message_data['pos']
            s = message_data['s']

            #send data to neighbor unless this is the right peer
            if identifier == peer_identifier:
                #store in local hash
                if pos not in local_hash:
                    local_hash[pos] = [event_row]
                else:
                    local_hash[pos].append(event_row)
                
                
            else:
                #send to right neighbor
                nextPeer = tuples[(identifier+1) % n]
                nextPeerAddress = (nextPeer[1], nextPeer[2])
                message_data = {
                    "command": "store",
                    "peer_identifier": peer_identifier,
                    "event_row": event_row,
                    "pos": pos,
                    "s": s
                }
                message_json = json.dumps(message_data)
                message_bytes = message_json.encode('utf-8')
                peerSocket.sendto(message_bytes, nextPeerAddress)
        
        elif command == "find-event":

            event_id = message_data['event_id']
            s_tuple = message_data['s_tuple']
            s_address = (s_tuple[1], s_tuple[2])

            #remove peer from I and add to id-seq
            I = message_data['I']
            if identifier in I:
                I.remove(identifier)
            id_seq = message_data['id_seq']
            id_seq.append(identifier)

            #compute the pos and id from the event id
            #check if you have the event-id in local hash
            #hash: pos = event_id mod s, id = pos mod n
            pos = event_id % s
            peer_id = pos % n

            if peer_id == identifier:
                
                #check if its in the local hash
                if pos in local_hash:
                    
                    #send 3-tuple to S (SUCCESS, event record, and set-id) else (FAILURE)
                    for row in local_hash[pos]:
                        
                        if int(row['EVENT_ID']) == event_id:       
                            event_row = row
                            event_success = {
                                "command": "find-event-result",
                                "result": "SUCCESS",
                                "record": event_row,
                                "id_seq": id_seq
                            }
                            event_bytes = json.dumps(event_success).encode('utf-8')
                            peerSocket.sendto(event_bytes, s_address)
                            break     
                else:
                    event_failure = {
                        "command": "find-event-result",
                        "result": "FAILURE",
                        "event_id": event_id
                    }
                    event_bytes = json.dumps(event_failure).encode('utf-8')
                    peerSocket.sendto(event_bytes, s_address)
                    
            #forwarded using hot-potato protocal
            else:
                next_peer_id = random.choice(I)
                #get peer from tuples
                next_peer = tuples[next_peer_id]
                next_peer_address = (next_peer[1], next_peer[2])

                forward_data = {
                    "command": "find-event",
                    "event_id": event_id,
                    "s_tuple": s_tuple,
                    "I": I,
                    "id_seq": id_seq
                }
                forward_bytes = json.dumps(forward_data).encode('utf-8')
                peerSocket.sendto(forward_bytes, next_peer_address)

        elif command == 'find-event-result':
            result = message_data['result']
            
            if result == "SUCCESS":
                record = message_data['record']
                print('SUCCESS')
                #print the record
                for key,val in record.items():
                    print(f"{key}: {val}")

                #print the suquence of peers visited
                id_seq = message_data['id_seq']
                print(f"id_seq: {id_seq}")
                    
            else:
                print(f"Storm event {message_data['event_id']} not found in the DHT")

        elif command == "teardown":

            if identifier == message_data['id']:
                #delete own local hash
                local_hash.clear()
                #teardown is completed so send message to manager
                name = tuples[identifier][0]
                m = f"teardown-complete {name}"
                
                identifier = None
                clientSocket.sendto(m.encode('utf-8'), server_address)

                #if this is part of a leave or join then no need to send teardown-complete
                if message_data['leave'] == 'yes':
                    continue
                else:
                    #expects either SUCCESS or FAILURE
                    data, _ = clientSocket.recvfrom(4096)
                    print(f"teardown-complete response: {data.decode('utf-8')}")

            else:
                #delete own local hash and send to its right neighbor
                local_hash.clear()

                next_id = (identifier+1) % n
                next_address = (tuples[next_id][1], tuples[next_id][2])
                message = {
                    "command": "teardown",
                    "id": message_data['id'],
                    "leave": message_data['leave']
                }
                identifier = None
                message_j = json.dumps(message)
                peerSocket.sendto(message_j.encode('utf-8'), next_address)
        
        elif command == "reset-id":
            #print(message_data)
            #set its new identifier, tuples, and n
            identifier = message_data['identifier']
            tuples = message_data['tuples']
            n = message_data['new-n']

            if message_data['function'] == 'leave':
                leaving_peer = message_data['leaving-peer']
            
            #send out set-id which goes around the ring and once it gets back to leader
            set_message = {
                    "command": "set-id",
                    "identifier": identifier+1,
                    "n": n,
                    "tuples": tuples,
                    "year": year,
                    "reset": 'yes'
            }
            #converts into json-formatted string
            message_json = json.dumps(set_message)
            #encodes this string into bytes
            message_bytes = message_json.encode('utf-8')
            #sends these bytes to its neighbor or (identifier+1)%n
            nextPeerAddress = (tuples[(identifier+1)%n][1], tuples[(identifier+1)%n][2])
            peerSocket.sendto(message_bytes, nextPeerAddress)

        else:
            print(f"Unkown command: {data}")
            

def next_prime(n):
    #brute force approach
    def is_prime(num):
        if num < 2:  # Numbers less than 2 are not prime
            return False
        for i in range(2, int(math.sqrt(num)) + 1):
            if num % i == 0:
                return False  # Found a divisor, num is not prime
        return True  # No divisors found, num is prime
    
    prime = n
    found = False
    while not found:
        prime += 1  # Try the next number
        if is_prime(prime):
            found = True  # Found a prime number
    return prime  # Return the found prime number


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