import socket
import sys
import random
import json
import threading
import csv

tuples = []
year = None
identifier = None
n = None
manager_address = None


def handle_manager_input(clientSocket, server_address):
    global n, year, identifier, tuples, manager_address
    manager_address = server_address
    while True:
        message = input()
        parts = message.split()
        clientSocket.sendto(message.encode(), server_address)


        if parts[0] == "register":
            data,_ = clientSocket.recvfrom(4096)
            print(data.decode('utf-8'))

        elif parts[0] == "setup-dht":
            data,_ = clientSocket.recvfrom(4096)
            print(data.decode('utf-8'))

            if data.decode('utf-8') == 'SUCCESS':
                n = int(parts[2])
                year = int(parts[3])

                for i in range(n):
                    data, _ = clientSocket.recvfrom(4096)
                    peer_info = data.decode('utf-8')
                    dataTuple = json.loads(peer_info)
                    tuples.append(dataTuple)

                #for t in tuples:
                    #print(t)

                #nextPeer ['name']['ipv4_address']['p_port']
                identifier = 0
                nextPeer = tuples[(identifier+1) % n]
                nextPeerAddress = (nextPeer[1], nextPeer[2])

                #print(f"next peer name: {nextPeer[0]}")
                #print(f"{nextPeerAddress}")
                # set-id <identifier> <n=ring size> <tuples>
                
                message_data = {
                    "command": "set-id",
                    "identifier": identifier+1,
                    "n": n,
                    "tuples": tuples
                }
                message_json = json.dumps(message_data)
                message_bytes = message_json.encode('utf-8')
                peerSocket.sendto(message_bytes, nextPeerAddress)
    

        

def handle_peer_socket(peerSocket):
    global identifier, n, tuples
    local_hash = {}
    

    while True:
        data, address = peerSocket.recvfrom(4096)
        #print(f"Received from peer: {data.decode()}")
        message_data = json.loads(data)
        command = message_data["command"]

        if command == "set-id":
            identifier = message_data["identifier"]
            #print(f"my identifier is {identifier}")
            n = message_data["n"]
            tuples = message_data["tuples"]

            if(identifier == 0):
                #print("Setup is complete")

                record_counter = {peer_id: 0 for peer_id in range(n)}
                #do hash functions
                file_name = '1950-1952/details-1950.csv'
                s = 449
                #get event_id
                with open(file_name, mode='r') as csvfile:
                    csvreader = csv.DictReader(csvfile)
                    for row in csvreader:
                        event_id = int(row["EVENT_ID"])
                        event_string = json.dumps(row)  # Convert row to a JSON string

                        #pos = event_id mod s
                        pos = event_id % s

                        #id = pos mod n
                        peer_id = pos % n
                        
                        #add to counter for num of records at each peer
                        record_counter[peer_id] += 1

                        if(peer_id == identifier):
                            local_hash[pos] = event_string
                        else:

                            message_data = {
                                "command": "store",
                                "peer_identifier": peer_id,
                                "event_string": event_string,
                                "pos": pos
                            }
                            message_json = json.dumps(message_data)
                            message_bytes = message_json.encode('utf-8')

                            next_identifier = (identifier+1) % n
                            nextPeer = tuples[next_identifier]
                            nextPeerAddress = (nextPeer[1], nextPeer[2])

                            peerSocket.sendto(message_bytes, nextPeerAddress)

                #print("Setup is complete")
                for peer_id, count in record_counter.items():
                    print(f"Peer ID: {peer_id}, Record Count: {count}")

                message = f"dht-complete {tuples[0][0]}"
                clientSocket.sendto(message.encode(), manager_address)
                data,_ = clientSocket.recvfrom(4096)
                data = data.decode('utf-8')
                print(data)


            else:
                #print(f"made it to {identifier}")
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

                #elif parts[0] == "SUCCESS":
                #peerSocket.sendto(b"SUCCESSFULLY RECEIVED", address)
        elif command == "store":
            # store <peer identifier> <event_string> <pos>
            peer_identifier = message_data['peer_identifier']
            event_string = message_data['event_string']
            pos = message_data['pos']
            #send data to neighbor unless this is the right peer
            if identifier == peer_identifier:
                #store in local hash
                local_hash[pos] = event_string
                #print("Stored in local hash")
            else:
                #send to neighbor
                #print(f"Currently at {identifier} trying to get to {peer_identifier}")
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

    serverIP = sys.argv[1]
    serverPort = int(sys.argv[2])
    server_address = (serverIP, int(serverPort))

    managerPort = random.randint(7001, 7499)
    peerPort = random.randint(7001, 7499)
    while managerPort == peerPort:
        peerPort = random.randint(7001, 7499)

    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    clientSocket.bind(('', managerPort))
    assigned_ip1, assigned_port1 = clientSocket.getsockname()


    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    peerSocket.bind(('', peerPort))
    assigned_ip2, assigned_port2 = peerSocket.getsockname()

    print(f"Manager socket bound to port {managerPort}, Peer socket bound to port: {peerPort}")

    hostname = socket.gethostname()
    IP = socket.gethostbyname(hostname)
    print(f"running on ip: {IP}")

    manager_thread = threading.Thread(target=handle_manager_input, args=(clientSocket, server_address))
    peer_thread = threading.Thread(target=handle_peer_socket, args=(peerSocket,))

    manager_thread.start()
    peer_thread.start()

    manager_thread.join()
    peer_thread.join()
