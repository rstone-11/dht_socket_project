import socket
import sys
import random
import json

portNum = sys.argv[1]
clients = []
selected_peers = []

#create a UDP socket
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

#bind the socket to the server address and port
server_address = ('', int(portNum)) #use '' to bind to all available interfaces
serverSocket.bind(server_address)

print("UDP server up and listening")
hostname = socket.gethostname()
IP = socket.gethostbyname(hostname)
print(f"running on ip: {IP}")

def dht_rebuilt(peerName, newLeader):
     return
     

#listen for incoming datagrams
while True:
    data, address = serverSocket.recvfrom(4096)

    message_text = data.decode('utf-8')
    parts = message_text.split()  #split by whitespace to get command parts

    #process the command
    if parts[0] == "register" and len(parts) == 5:
        #extract parameters from the command
        peer_name, ipv4_address, m_port, p_port = parts[1:]

        #process registration
        if any(client['peer_name'] == peer_name for client in clients):
                print(f"peer name is not unique")
                serverSocket.sendto(b"FAILURE",(ipv4_address, int(m_port)))
                continue
                #peer name is not unique, so registration fails
        else:
            #if unique, add the new peer
            clients.append({
                'peer_name': peer_name,
                'ipv4_address': ipv4_address,
                'm_port': int(m_port),
                'p_port': int(p_port),
                'state': 'Free'
            })
            print(f"Registering peer: {peer_name}, Address: {ipv4_address}, M-port: {m_port}, P-port: {p_port}")
            serverSocket.sendto(b"SUCCESS",(ipv4_address, int(m_port)))



    elif parts[0] == "setup-dht" and len(parts) == 4:
        tuples = None
        peer_name, n, YYYY = parts[1:]
        n = int(n)
        
        #check if peer-name is registered
        if not any(client['peer_name'] == peer_name for client in clients):
             serverSocket.sendto(b"FAILURE", address)
             print(f"peer name is not registered")
             continue

        #n is at least 3
        if n < 3:
             serverSocket.sendto(b"FAILURE", address)
             print(f"n isn't at least 3")
             continue

        #at least n users are registered
        if len(clients) < n:
             serverSocket.sendto(b"FAILURE", address)
             print(f"there aren't {n} clients")
             continue

        #a dht has not already been setup
        if any(client['state'] != 'Free' for client in clients):
             serverSocket.sendto(b"FAILURE", address)
             print("a dht has already been setup")
             continue
        
        print(f"Setting up DHT for peer: {peer_name}, N: {n}, YYYY: {YYYY}")

        leader = None
        leader_address = None

        #manager sets state of peer-name to Leader of one who is specified in command
        for client in clients:
             if client['peer_name'] == peer_name:
                client['state'] = 'Leader'
                leader = client  
                leader_address = (client['ipv4_address'], client['m_port'])
                serverSocket.sendto(b"SUCCESS", (client['ipv4_address'], client['m_port']))
        print(f'leader of current dht is {leader} at {leader_address}')
            
        #selects n-1 Free users from registered peers and changes each one’s state to InDHT
        selected_peers = [leader] + random.sample([client for client in clients if client['state'] == 'Free'], n-1)
        for peer in selected_peers[1:]:
             peer['state'] = 'InDHT'
     
        for peer in selected_peers:
             for client in clients:
                  if client['peer_name'] == peer['peer_name']:
                       client['state'] = peer['state']
             
        print('client list:')
        for client in clients:
             print(client)

        print('selected_peers:')
        for peer in selected_peers:
             print(peer)

        #sends each peer in the dht individually to the leader
        tuples = [[peer['peer_name'], peer['ipv4_address'], peer['p_port']] for peer in selected_peers]
        for tuple in tuples:
             peer_info = json.dumps(tuple)
             serverSocket.sendto(peer_info.encode('utf-8'), leader_address)

        #waits for the dht-complete command
        data, address = serverSocket.recvfrom(4096)
        message_text = data.decode('utf-8')
        parts = message_text.split()  #split by whitespace to get command parts

        if parts[0] == "dht-complete" and len(parts) == 2:
          peer_name = parts[1]
          if selected_peers[0]['peer_name'] == peer_name:
               # Mark DHT as complete for this peer
               print(f"DHT complete for peer: {peer_name}")
               serverSocket.sendto(b"SUCCESS", address)

          else: 
               print('peer that sent dht-complete wasn\'t leader')
               serverSocket.sendto(b"FAILURE", address)
        else:
             serverSocket.sendto(b"FAILURE", address)

    elif parts[0] == "query-dht" and len(parts) == 2:
         #make sure dht is setup
         if not selected_peers:
              print('dht is not setup')
              serverSocket.sendto(b"FAILURE", address)
              continue

         #make sure S is registered and free
         name = parts[1]
         #print(f"looking for: {name} in clients")
         peer_S = None
         for client in clients:
              if client['peer_name'] == name:
                   peer_S = client

         if peer_S is None:
              print('client is not registered')
              serverSocket.sendto(b"FAILURE", address)
              continue

         if peer_S['state'] != 'Free':
              print('client is registered but not free')
              serverSocket.sendto(b"FAILURE", address)
              continue     

         #choose one of the peers in the DHT at random
         random_peer = random.choice(selected_peers)

         #sends a 3-tuple of this peers info back to S and S's name and ip
         r_tuple = random_peer['peer_name'], random_peer['ipv4_address'], random_peer['p_port'], peer_S['peer_name'], peer_S['ipv4_address'], len(selected_peers)
         r_tuple = json.dumps(r_tuple)
         serverSocket.sendto(r_tuple.encode('utf-8'), address)

    elif parts[0] == 'deregister' and len(parts) == 2:
         peer_t = None
         #make sure the peer is in a Free state
         peer_name = parts[1]
         for client in clients:
              if client['peer_name'] == peer_name:
                   peer_t = client
          
         #if so then remove peer from clients
         if peer_t is not None and peer_t['state'] == 'Free' and peer_t not in selected_peers:
              clients.remove(peer_t)
              serverSocket.sendto(b"SUCCESS", address)

              print('updated client list:')
              for client in clients:
                   print(client)

              print('current selected_peers: ')
              for peer in selected_peers:
                   print(peer)
         #if not then return FAILURE
         else:
              print('peer was not free')
              serverSocket.sendto(b"FAILURE", address)

    elif parts[0] == 'teardown-dht' and len(parts) == 2:
         #print(f'here is the clients: {clients}')
         #print(f'here is the selected_peers: {selected_peers}')

         #make sure name is leader of the dht
         peer_t = None
         peer_name = parts[1]
         #print(f'peer name is {peer_name}')
         for client in clients:
              if client['peer_name'] == peer_name:
                   peer_t = client

         if peer_t is not None and peer_t['state'] == 'Leader' and peer_t in selected_peers:
              serverSocket.sendto(b"SUCCESS", address)
              print('waiting for teardown-complete')
              #manager waits for teardown-complete message
              data, address = serverSocket.recvfrom(4096)
              
              message = data.decode('utf-8')
              #print(f'got the message: {message}')
              result_message = message.split()
              #print(f'result_message: {result_message}')

              if result_message[0] == 'teardown-complete' and len(result_message) == 2:
                   #print('made it into if stmt')
                   #now check if name is the leaders
                   peer_t = None
                   for client in clients:
                        if client['peer_name'] == parts[1]:
                             peer_t = client
                        
                   #print(f'peer_t is {peer_t}')
                   #change state of each peer to Free
                   if peer_t is not None and peer_t['state'] == 'Leader' and peer_t in selected_peers:
                        #change state to Free and return success
                        #for peer in selected_peers:
                         #    selected_peers.remove(peer)   
                        selected_peers.clear()
                        for client in clients:
                              client['state'] = 'Free'

                        print(f'clients: {clients}')
                        print(f'selected_peers: {selected_peers}')

                        #respond with success
                        serverSocket.sendto(b"SUCCESS", address)
                        
                   else:
                        print('peer that send teardown-complete is not leader')
                        serverSocket.sendto(b"FAILURE", address)
              else:
                   print('manager did not receive teardown-complete')
                   print(f'received {message}')
                   #failure
                   serverSocket.sendto(b"FAILURE", address)
               
         else:
              if peer_t is None:
                   print('peer is not in client list')
              elif peer_t['state'] != 'Leader':
                   print(f"peer is not leader its {peer_t['state']}")
                   print(f"here is the peer: {peer_t}")
              elif peer_t not in selected_peers:
                   print('peer is not in selected peers, here is current list: ')
                   for peer in selected_peers:
                        print(peer)
              else:
                   print(f'none fit here is peer_t: {peer_t}')
                   
              print('peer that sent teardown-dht is not the leader')
              serverSocket.sendto(b"FAILURE", address)

    elif parts[0] == "leave-dht" and len(parts) == 2:
         peer_name = parts[1]
         peer_l = None


         if len(selected_peers) < 4:
              print("DHT wouldn't be at least three if one left")
              serverSocket.sendto(b"FAILURE", address)
              continue

         #check if DHT exists
         if not any(client['state'] == 'Leader' for client in clients):
             serverSocket.sendto(b"FAILURE", address)
             print("a dht has already been setup")
             continue

         leaving_peer_id = None
         #check if peer is maintaining DHT
         for i, peer in enumerate(selected_peers):
              if peer['peer_name'] == peer_name:
                   peer_l = peer
                   leaving_peer_id = i
         print(f"leaving peer is {peer_l['peer_name']} with id of {leaving_peer_id}")
          
         if peer_l is None or leaving_peer_id is None:
              #respond with FAILURE
              serverSocket.sendto(b"FAILURE", address)
              continue
         
         if peer_l['state'] == 'InDHT' or peer_l['state'] == 'Leader':
              #respond with SUCCESS
              print(f"Sending SUCCESS to {address}")
              serverSocket.sendto(b"SUCCESS", address)
              
              data, address = serverSocket.recvfrom(4096)
              #waits for teardown complete
              message = data.decode('utf-8')
              #print(f'got the message: {message}')
              result_message = message.split()
              #print(f'result_message: {result_message}')

              if result_message[0] == 'teardown-complete' and len(result_message) == 2:
                  #return SUCCESS to leaving peer
                  serverSocket.sendto(b"SUCCESS", address)

                  #at manager we need to remove the peer from selected_peers who initiated leave
                  #we then need to recreate selected_peers with new leader who is (identifier+1)%n of leaving peer
                  #we then send this new selected_peers to the new leader so he can assign the new identifiers

                  newLeader = selected_peers[(leaving_peer_id+1)%n]

                  #remove the leaving peer from tuples and set its client state to 'Free'
                  n = n-1
                  for client in clients:
                       if client['peer_name'] == peer_l['peer_name']:
                            client['state'] = 'Free'
                       if client['peer_name'] == newLeader['peer_name']:
                            client['state'] = 'Leader'

                  #need to get the leaving peer's right neighbor
                  #then clear the selected_peers and rebuild it
                  selected_peers.clear()

                  print(f"new leader will be {newLeader['peer_name']}")

                  # Generate a pool of clients excluding the newLeader explicitly
                  client_pool = [client for client in clients if client != newLeader and (client['state'] == 'InDHT' or client['state'] == 'Leader')]

                  # Create the selected_peers list with newLeader and a sample from the pool
                  selected_peers = [newLeader] + random.sample(client_pool, n-1)
                  for peer in selected_peers[1:]:
                    if peer['state'] == 'Leader':
                         peer['state'] = 'InDHT'
     
                  print('client list after teardown')
                  for client in clients:
                       print(client)

                  print('selected_peer list after teardown')
                  for peer in selected_peers:
                       print(peer)

                  tuples.clear()
                  tuples = [[peer['peer_name'], peer['ipv4_address'], peer['p_port']] for peer in selected_peers]
                  print(f"n: {n}")
                  print(f"tuples that are sent, with a size of {len(tuples)} ")
                  for tuple in tuples:
                    print(tuple)
                    peer_info = json.dumps(tuple)
                    serverSocket.sendto(peer_info.encode('utf-8'), address)

                  #waits for dht-rebuilt
                  data, address = serverSocket.recvfrom(4096)
                  message = data.decode('utf-8')
                  parts = message.split()

                  if parts[0] == 'dht-rebuilt' and len(parts) == 3:
                    continue
                  else:
                    continue

         else:
               #respond with FAILURE
              serverSocket.sendto(b"FAILURE", address)
              
    
    elif parts[0] == "join-dht" and len(parts) == 2:
         peer_name = parts[1]
         peer_j = None

         #check if DHT exists
         if any(client['state'] != 'Free' for client in clients):
             serverSocket.sendto(b"FAILURE", address)
             print("a dht has already been setup")
             continue
         
         #check if peer is free
         for client in clients:
              if client['peer_name'] == peer_name:
                   peer_j = client

         if peer_j is not None and peer_j['state'] == 'Free':
              #respond with SUCCESS
              serverSocket.sendto(b"SUCCESS", address)
              
              #waits for dht-rebuilt
              data, address = serverSocket.recvfrom(4096)
              message = data.decode('utf-8')
              parts = message.split()

              if parts[0] == 'dht-rebuilt' and len(parts) == 3:
                    continue
              else:
                    continue

         else:
               #respond with failure
               serverSocket.sendto(b"FAILURE", address)
               
        
          
    else:
          print(f"Unknown command: {parts}")
          serverSocket.sendto(b"FAILURE", address)
