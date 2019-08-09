"""

myapp.py
----------------------
Built on top of CMPS 128 Assignments
A Sharded, Fault Tolerant Key-Value store by replication that provides causal consistency

"""
from flask import Flask, request, jsonify, make_response, Response
from flask_executor import Executor
import asyncio
import os, sys
import time
import requests
import threading
import random
import hashlib
app = Flask(__name__)
################################## GLOBAL VARIABLES #########################################

running_ip = [] # store view of this replica
this_ip = None  # store self socket address
history = []  # EX: ("key?10.10.0.3:8080:1", "mykey", "myval", "causal-metadata")
counter = 0 # counter for version number 
shard_count = 0 # store external shard_count
my_shard = 0 # store self shard id
ping_leader = None
ping_watcher = None 

##############################################################################################

################################# ERROR HANDLERS #############################################

@app.errorhandler(404)
def bad_view_request(e):
    if e is 'PUT':
        resp = jsonify(error='Socket address already exists in the view', message='Error in PUT')
    elif e is 'DELETE':
        resp = jsonify(error='Socket address does not exist in the view', message='Error in DELETE')
    resp.status_code = 404
    return resp

@app.errorhandler(400)
def bad_request(e):
    if e is 'GET':
        resp = jsonify(error='Key does not exist', message='Error in GET')
    if e is 'DELETE':
        resp = jsonify(error='Key does not exist', message='Error in DELETE')
    if e is 'SHARD':
        resp = jsonify(message='Not enough nodes to provide fault-tolerance with the given shard count!')
    resp.status_code = 400
    return resp

##############################################################################################
####################################### APP INIT #############################################
def check_view_list():
    app = Flask(__name__)
    # need to set global keyword to avoid ambiguity
    global this_ip
    global running_ip
    global history
    global shard_count
    global my_shard
    global ping_leader
    global ping_watcher

    # get external view and socket_address data
    running_ip = os.environ.get('VIEW').split(',') # a list of string of running ip, including itself
    this_ip = str(os.environ.get('SOCKET_ADDRESS')) # get self ip address
    # assign ping leader and ping watcher 
    if len(running_ip) > 1:
        ping_leader = running_ip[0] 
        ping_watcher = running_ip[-1]   
    # get external shard count
    # check if SHARD_COUNT is provided
    if os.environ.get('SHARD_COUNT') is not None:
        shard_count = int(os.environ.get('SHARD_COUNT'))
        if shard_count * 2 <= len(running_ip):
            # assign shard id to node using position mod shard_count
            my_shard = ((running_ip.index(this_ip) + 1) % shard_count) + 1
        else:
            raise ValueError("not enough nodes for this shard count")
    else:
        pass # if SHARD_COUNT is not provided, need to add to a shard through add-member

    # do a PUT broadcast to every other running ip in it's view
    def view_put_broadcast():
        for ip in running_ip:
            if ip is this_ip:
                continue
            socket_add = {'socket-address': this_ip}
            try:  
                resp = requests.put('http://' + str(ip) + '/key-value-store-view',json=socket_add)  
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print("this IP is already in the node's view list")

    thread = threading.Thread(target=view_put_broadcast)
    thread.start()

    # retrive all key-value pairs from one of the nodes from the same shard and put them into local store
    # TODO: is there a way to not go through these requests when the nodes haven't received any request and history is empty
    if len(running_ip) > 1:
        global history
        random_ip = None
        # do not retrive history if shard id is unkown
        if my_shard!=0:
            for ip in running_ip:
                if ip is this_ip:
                    continue 
                try:
                    rand = requests.get('http://' + str(ip) + '/key-value-store-shard/node-shard-id')
                except (requests.Timeout, requests.exceptions.RequestException) as e:
                    pass
                else:
                    result_id = int(rand.json().get('shard-id'))
                    if result_id == my_shard:
                        random_ip = ip
                        break
            try:
                resp = requests.get('http://' + str(random_ip) + '/history')             
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                pass
            else:
                history = resp.json().get('history')
    return app

app = check_view_list() # assign the app to initialize
executor = Executor(app) # wrap it in a coroutine executor 

############################################ VIEW PINGING MECHANICS #############################################
# this function is called before the first request is made 
# @app.before_first_request
# def call_ping_subroutine():
#     # subroutine called to check other nodes' availability 
#     executor.submit(start_pinging)  


# if this node is the leader, ping all the nodes; if the node is the watcher, ping only the leader; otherwise, pass 
# the check is called every 5 seconds, with a 15 second delay at the beginning 
def start_pinging() -> None:
    time.sleep(15)
    while True:
        if this_ip == ping_leader:
            ping_all_nodes()
        elif this_ip == ping_watcher:
            ping_the_leader() 
        else:
            pass
        
        time.sleep(5) 

# as the leader, ping all the nodes, including the watcher
def ping_all_nodes() -> None:
    global ping_watcher
    for ip in running_ip:
        if ip == this_ip:
            continue
        try:
            # allow a maximum 2 seconds of response time 
            resp = requests.get('http://' + str(ip) + '/ping', timeout=2)
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            # remove irresponsive node from its own view list
            running_ip.remove(ip)   
            # if the ping watcher is down, assign new ping watcher randomly 
            if ip is ping_watcher:
                ping_watcher = running_ip[-1] if running_ip[-1]!= this_ip else running_ip[-2] 
                # send a put request to tell the new ping watcher about its new role 
                res = requests.put('http://' + str(ping_watcher) + '/new-watcher')

            # send delete broadcast to delete the irresponsive node from others' view list  
            for good_ip in running_ip:
                if good_ip is this_ip:
                    continue  
                ip_to_remove = {'socket-address': ip}
                try:
                    
                    res = requests.delete('http://' + str(good_ip) + '/key-value-store-view', json=ip_to_remove) 
                except (requests.Timeout, requests.exceptions.RequestException) as e:
                    pass
    return None 
   
# as the watcher, ping the leader 
def ping_the_leader() -> None:
    global ping_leader
    global ping_watcher 
    
    try:
        # allow a maximum 2 seconds of response time 
        resp = requests.get('http://' + str(ping_leader) + '/ping', timeout=2) 
    except (requests.Timeout, requests.exceptions.RequestException) as e:
        # if the leader is irresponsive, remove leader from view list
        running_ip.remove(ping_leader)       
        # broadcast delete request to all remaining nodes
        for good_ip in running_ip:
            if good_ip is this_ip:
                continue
            ip_to_remove = {'socket-address': ping_leader} 
            try:         
                res = requests.delete('http://' + str(good_ip) + '/key-value-store-view', json=ip_to_remove) 
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                pass
        # take the leader role from the deceased
        ping_leader = this_ip 
        # assign a new watcher  
        for ip in running_ip:
            if ip != ping_leader:
                ping_watcher = ip 
                break 
        # send put request to the node for its new role 
        res = requests.put('http://' + str(ping_watcher) + '/new-watcher')   
    
    return None 


"""
Endpoint: /new-watcher
URL_for: new_watcher
Purpose: Let a node knows that it has become the new watcher 
Accessed by: Another node. Client is unauthorized 
"""
@app.route('/new-watcher', methods=['PUT']) 
def new_watcher():
    if request.remote_addr + ":8080" not in running_ip:
        resp = jsonify(message='You are not authorized to access this endpoint')
        resp.statue_code = 401 
        return resp 

    global ping_watcher
    global ping_leader  
    ping_watcher = this_ip 
    ping_leader = request.remote_addr + ":8080" 
    resp = jsonify(message='New watcher is assigned')
    resp.status_code = 200
    return resp 

"""
Endpoint: /ping
URL_for: ping
Purpose: Check if a node is alive or not x_x  
Accessed by: Client or other nodes 
"""
@app.route('/ping', methods=['GET'])
def ping():
    node_num = int(this_ip[-1])-1  
    resp = jsonify('Node' + str(node_num) + 'is responsive') 
    resp.statue_code = 200
    return resp 

########################################################################################################

######################################## SHARDS OPERATIONS #############################################

"""
Endpoint: /key-value-store-shard/shard-ids
URL_for: shard_ids
Purpose: Get the list of active shard ids in the system  
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def shard_ids():
    return return_shard_ids()

def return_shard_ids():
    shards = []
    for i in range(1, (shard_count+1)):
        shards.append(i)
    shard_ids = ','.join([str(shard) for shard in shards])
    message = jsonify(**{'message':'Shard IDs retrieved successfully', 'shard-ids':shard_ids})
    resp = make_response(message, 200)
    return resp

#===========================================================================================================

"""
Endpoint: /key-value-store-shard/node-shard-id
URL_for: node_shard_id
Purpose: Get the list of active shard ids in the system  
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def node_shard_id():
    return return_node_shard_id()

def return_node_shard_id():
    global my_shard
    shard_id = str(my_shard)
    message = jsonify(**{'message':'Shard ID of the node retrieved successfully', 'shard-id':shard_id})
    resp = make_response(message, 200)
    return resp

#===========================================================================================================
"""
Endpoint: /key-value-store-shard/shard-id-members/<shard_id>
URL_for: shard_members, shard_id=? 
Purpose: Get all members of a shard id, return list of members' socket address  
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=['GET'])
def shard_members(shard_id):
    global my_shard
    members = []
    shard_id = int(shard_id)
    # check shard_id of all nodes
    for ip in running_ip:
        try:
            resp = requests.get('http://' + str(ip) + '/key-value-store-shard/node-shard-id') 
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            pass
        else:
            result = int(resp.json().get('shard-id'))  
            if result == shard_id:
                members.append(ip)

    return return_shard_members(members)

def return_shard_members(members):
    shard_id_members = ','.join(members)
    message = jsonify(**{'message':'Members of shard ID retrieved successfully', 'shard-id-members':shard_id_members})
    resp = make_response(message, 200)
    return resp

#===========================================================================================================
"""
Endpoint: /key-value-store-shard/shard-id-key-count/<shard_id>
URL_for: shard_id_key_count, shard_id=? 
Purpose: return number of keys stored in a shard  
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store-shard/shard-id-key-count/<shard_id>', methods=['GET'])
def shard_id_key_count(shard_id):
    key_count = []
    forward_ip = None
    if my_shard == int(shard_id):
        for his in history:
            if his[1] not in key_count and his[2] is not None:
                key_count.append(his[1])
            elif his[1] in key_count and his[2] is None:
                key_count.remove(his[1])
        return return_key_count(len(key_count))

    else:
        for ip in running_ip:
            if ip is not this_ip:
                try:
                    resp = requests.get('http://' + str(ip) + '/key-value-store-shard/node-shard-id')
                except (requests.Timeout, requests.exceptions.RequestException) as e:
                    pass
                else:
                    result = int(resp.json().get('shard-id'))
                    if result == int(shard_id):
                        forward_ip = ip
                        break
        try:
            forw = requests.get(request.url.replace(request.host_url, 'http://' + str(forward_ip) + '/'))
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            abort(401) 
        return Response(forw.content, forw.status_code)

def return_key_count(count):
    key_count = str(count)
    message = jsonify(**{'message':'Key count of shard ID retrieved successfully', 'shard-id-key-count':key_count})
    resp = make_response(message, 200)
    return resp

#===========================================================================================================
"""
Endpoint: /key-value-store-shard/add-member/<shard_id>
URL_for: add_member, shard_id=? 
Purpose: Add a new member to shard   
Accessed by: Client  
"""
@app.route('/key-value-store-shard/add-member/<shard_id>', methods=['PUT'])
def add_member(shard_id):
    global this_ip
    global my_shard
    global running_ip
    global shard_count
    
    random_ip = None

    # get socket_address of the new node to be added to shard_id
    result = request.get_json(force=True)
    new_add = result['socket-address']

    # check if we are in new node
    if new_add is this_ip:
        # assign shard id
        my_shard = shard_id
        # get shard count data from a random ip
        for ip in running_ip:
            if ip is not this_ip:
                random_ip = ip
                break
        try:
            resp = requests.get('http://' + str(random_ip) + '/key-value-store-shard/shard-ids')
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            pass
        else:
            shard_ids = resp.json().get('shard-ids').split(',')
            shard_count = int(shard_ids[-1])


        # retrive history and return message
        return return_add_member(shard_id)

    # if this is not the new node, forward request to new node
    # and return the response
    else:
        try:
            resp = requests.request(
                method = 'PUT',
                url = request.url.replace(request.host_url, 'http://' + str(new_add) + '/'),
                json = request.get_json(force=True)
            )
            return Response(resp.content, resp.status_code)
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            print('Error in shard add member (new node dead)')

def return_add_member(shard_id):
    global history

    # select a node within the same shard id
    get_add = None
    for ip in running_ip:
        if ip is not this_ip:
            try:
                resp = requests.get('http://' + str(ip) + '/key-value-store-shard/node-shard-id') 
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('error in return add member')
            else:
                node_shard_id = int(resp.json().get('shard-id'))
                if node_shard_id == shard_id:
                    get_add = ip
                    break

    
    # retrive history data from this node
    try:
        resp = requests.get('http://' + str(get_add) + '/history')
        history = resp.json().get('history')
    except (requests.Timeout, requests.exceptions.RequestException) as e:
        print('error in return add member retrive history')

    # return message
    message = jsonify(message='Add member successfully')
    resp = make_response(message, 200)
    return resp

#===========================================================================================================
"""
Endpoint: /key-value-store-shard/reshard
URL_for: reshard_keys
Purpose: Reshard the store based on the given shard count    
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard_keys():
    result = request.get_json(force=True) 
    reshard_number = int(result['shard-count']) 
    # check if there's sufficient replica to do the requested number of shards
    # there has to be at least two replicas in a shard
    if len(running_ip)  < reshard_number * 2:
        return bad_request('SHARD')
    elif reshard_number == shard_count:
        resp = jsonify(message='This is the original shard number, no actions to be done') 
        resp.status_code = 200
        return resp 
    # run the reshard process concurrently in the background task queue
    executor.submit(reshard, reshard_number) 
    # return response cuz the client is waitingggg
    time.sleep(3)
    resp = jsonify(message='Resharding processed successfully')
    resp.status_code = 200
    return resp

"""
Endpoint: /history
URL_for: kvs_his
Purpose: Get the kvs history from a node     
Accessed by: Client or other node 
"""
@app.route('/history', methods=['GET'])
def kvs_his():
    resp = jsonify(message='Retreive key value store', history=history)
    resp.status_code = 200
    return resp


"""
Endpoint: /key-value-store-shard/reshard_history
URL_for: reshard_keys_from_replica
Purpose: Update the node with its new history, shard id, and shard count     
Accessed by: Other node. Client is unauthorized 
"""
# the function for overriding a node's kvs, shard id, and shard count knowledge 
@app.route('/key-value-store-shard/reshard_history', methods=['PUT'])
def reshard_keys_from_replica():
    global history 
    global my_shard
    global shard_count
    request_source = request.remote_addr + ":8080"
    # security check, if this request is not from another node, it can be dangerous 
    if request_source not in running_ip:
        abort(401)
    # get a bunch of data
    result = request.get_json(force=True)
    history = result['new_history']
    my_shard = int(result['new_id'])
    shard_count = int(result['new_count'])
    resp = jsonify(message="Success")
    resp.status_code = 201
    return resp

def reshard(reshard_number) -> None:
    global history 
    global shard_count
    global my_shard
    global running_ip
    global this_ip
    # initialize a list for all kvs across shards, with its own list of shard history as default
    all_kvs = history 
    # initialize a list for all shard ids, also has its own id as default
    all_ids = [my_shard] 
    # search through all the nodes
    for ip in running_ip:
        if this_ip is not ip:
            # get the shard id of this ip 
            try: 
                resp = requests.get('http://' + str(ip) + '/key-value-store-shard/node-shard-id')
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                pass
            else:
                this_shard_id = int(resp.json().get('shard-id'))
                if this_shard_id not in all_ids:
                    all_ids.append(this_shard_id)
                    try:
                        resp = requests.get('http://' + str(ip) + '/history')
                    except (requests.exceptions.RequestException) as e:
                        pass
                    else:
                        history_from_ip = resp.json().get('history') 
                        all_kvs = all_kvs + history_from_ip
                        if len(all_ids) == shard_count:
                            break

    # # initialize a new dict for resharding kvs
    data = {i: [] for i in range(1, reshard_number+1)} 
    for item in all_kvs:
        # find the key's new corresponding shard id 
        new_shard_id = ( hash_a_string(item[1]) % reshard_number) + 1
        # add the item to the dict list for it 
        data[new_shard_id].append(item)
    
    count = 1
    for ip in running_ip:
        ip_new_shard_id = count
        count = count  % reshard_number + 1 
        if ip != this_ip:
            # evenly assign new shard id for each node, to ensure each shard has at least two nodes 

            forwarding_data = {'new_history': data[ip_new_shard_id], 
                               'new_id': str(ip_new_shard_id),
                               'new_count': str(reshard_number)} 
            # send request to this ip to put the new history, shard count, and shard id into it, override the old 
            try:
                resp = requests.put('http://' + str(ip) + '/key-value-store-shard/reshard_history', json=forwarding_data)
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                pass
        else:
            my_shard = ip_new_shard_id
            shard_count = reshard_number
            history = data[ip_new_shard_id] 

    return None # success
#####################################################################################################

######################################## KVS OPERATIONS #############################################
"""
Endpoint: /key-value-store/<key>, method=PUT
URL_for: kvs_put, key=?
Purpose: 1) Add or update a key-value pair in the store     
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store/<key>', methods=['PUT'])
def kvs_put(key):
    #--------------------------key hashing-------------------------------------------
    new_shard_id = hash_a_string(key) % shard_count + 1 
    # print("This is hash of key", hash_a_string(key))
    # print("From node", this_ip)
    #-----------------------end key hashing--------------------------------------------
    # get request ip, to check if it's from client
    request_source = request.remote_addr + ":8080"
    # ---------------------------get the data passed in-----------------------------
    result = request.get_json(force=True)
    value = result['value']
    cm = result['causal-metadata'] # EX: "key1?10.10.0.2:1,key2?10.10.0.2:2"
    
    # if this is the "correct" shard, do request normal
    if new_shard_id == my_shard:
        # When the dependencies are not satisfied, wait
        # Flask can process request concurrently by default
        # Need to check for causal consistency if there are causal-metadata passed in
        if cm:
            cm_list = cm.split(',') # EX: [key1?10.10.0.2:1, key2?10.10.0.2:2]
            # loop through causal metadata
            for item in cm_list:
                cm_key = item.split('?')[0] # get key in every causal metadata
                cm_version = item.split('?')[1] # get version in every causal metadata
                cm_shard = hash_a_string(cm_key) % shard_count + 1 # calculate the shard id which the causal metadata is stored
                # if the causal metadata should be stored in this shard
                if cm_shard == my_shard:
                    # halt until this causal metadata is in history
                    while item not in [his[0] for his in history]:
                        time.sleep(1)
                # else, need to check the "correct" shard id
                else:
                    forwarding_ip = kvs_first_member(cm_shard)
                    try:
                        resp = requests.get('http://' + str(forwarding_ip) + '/history')
                    except (requests.Timeout, requests.exceptions.RequestException) as e:
                        return Response(status=404)
                    else:
                        forward_history = resp.json().get('history')
                    while item not in [his[0] for his in forward_history]:
                        time.sleep(1)
                        forwarding_ip = kvs_first_member(cm_shard)
                        try:
                            resp = requests.get('http://' + str(forwarding_ip) + '/history')
                        except (requests.Timeout, requests.exceptions.RequestException) as e:
                            return Response(status=404)
                        else:
                            forward_history = resp.json().get('history')
                    
        # if request from client or from another node or if request id forwarded from another node
        if request_source not in running_ip or 'from-shard' in result:
            return put_op_from_client(request, key, value, cm, new_shard_id)
        # if request id forwarded from another node
        else:
            return op_from_replica(request, key, result)

    # else, forward the request to the "correct" shard member
    else:
        forwarding_ip = kvs_first_member(new_shard_id)
        forwarding_data = {'value': value, 'causal-metadata': cm, 'from-shard': 1}
        try:
            forw = requests.put(request.url.replace(request.host_url, 'http://' + str(forwarding_ip) + '/'), json = forwarding_data)
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            return Response(status=404)
        else:    
            return Response(forw.content, forw.status_code)

# function for put operation from client
# return response
def put_op_from_client(request, key, value, cm, shard_id):
    global my_shard
    version = generate_version(key)
    forwarding_data = {'version': version, 'causal-metadata': cm, 'value': value}
    executor.submit(broadcast_request, request, forwarding_data, kvs_shard_members(shard_id))
    # expand the causal metadata containing coresponding key to the version
    updated_cm = version if cm is '' else cm + ',' + version
    shard_id = str(shard_id)
    if key in [ item[1] for item in history ]:
        resp = jsonify(**{'message':'Updated successfully', 'version':version, 'causal-metadata':updated_cm, 'shard-id':shard_id})
        resp.status_code = 200
    else:
        resp = jsonify(**{'message':'Added successfully', 'version':version, 'causal-metadata':updated_cm, 'shard-id':shard_id})
        resp.status_code = 201
    add_element_to_history( version, key, value, updated_cm )
    return resp

#===========================================================================================================

"""
Endpoint: /key-value-store/<key>, method=DELETE
URL_for: kvs_del, key=?
Purpose: 1) Delete a key-value pair in the store     
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store/<key>', methods=['DELETE'])
def kvs_del(key):
    new_shard_id = hash_a_string(key) % shard_count + 1 
    # DELETE request delete the corresponding key-value pair
    # get request ip, to check if it's from client
    request_source = request.remote_addr + ":8080"
    # ---------------------------get the data passed in-----------------------------
    result = request.get_json(force=True)
    value = None
    cm = result['causal-metadata']

    # if this is the "correct" shard, do request normal
    if new_shard_id == my_shard:
        if cm:
            cm_list = cm.split(',') # EX: [key1?10.10.0.2:1, key2?10.10.0.2:2]
            # loop through causal metadata
            for item in cm_list:
                cm_key = item.split('?')[0]
                cm_version = item.split('?')[1]
                cm_shard = hash_a_string(cm_key) % shard_count + 1
                if cm_shard == my_shard:
                    while item not in [his[0] for his in history]:
                        time.sleep(1)
                else:
                    forwarding_ip = kvs_first_member(cm_shard)
                    try:
                        resp = requests.get('http://' + str(forwarding_ip) + '/history')
                    except (requests.Timeout, requests.exceptions.RequestException) as e:
                        return Response(status=404)
                    else:
                        forward_history = resp.json().get('history')
                    while item not in [his[0] for his in forward_history]:
                        time.sleep(1)
                        forwarding_ip = kvs_first_member(cm_shard)
                        try:
                            resp = requests.get('http://' + str(forwarding_ip) + '/history')
                        except (requests.Timeout, requests.exceptions.RequestException) as e:
                            return Response(status=404)
                        else:
                            forward_history = resp.json().get('history')

        # loop through history to find the latest value of the key
        for item in history:
            if item[1] == key:
                value = item[2]

        # if the last item that correspond to the key is not None
        if value != None:
            # if the request is from client
            if request_source not in running_ip:
                return delete_op_from_client(request, key, cm, new_shard_id)
            # if the request is forwarded from another node
            elif 'from-shard' in result:
                return delete_op_from_client(request, key, cm, new_shard_id)
            else:
                return op_from_replica(request, key, result)
        # if value of the key is null, return error message
        return bad_request('DELETE')

    # else, forward the request to the "correct" shard member
    else:
        forwarding_ip = kvs_first_member(new_shard_id)
        forwarding_data = {'causal-metadata': cm, 'from-shard': 1}
        try:
            forw = requests.delete(request.url.replace(request.host_url, 'http://' + str(forwarding_ip) + '/'), json = forwarding_data)
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            return Response(status=404)
        else:
            return Response(forw.content, forw.status_code)

# function for deleting key operation from client
# return response
def delete_op_from_client(request, key, cm, shard_id):
    value = None
    version = generate_version(key)
    forwarding_data = {'version': version, 'causal-metadata': cm, 'value': value }
    executor.submit(broadcast_request, request, forwarding_data, kvs_shard_members(shard_id))
    # expand the causal metadata containing coresponding key to the version
    updated_cm = version if cm is '' else cm + ',' + version
    shard_id = str(shard_id)
    resp = jsonify(**{'message':'Deleted successfully', 'version':version, 'causal-metadata':updated_cm, 'shard-id': shard_id})
    resp.status_code = 200
    add_element_to_history( version, key, value, updated_cm )
    return resp


#===========================================================================================================

"""
Endpoint: /key-value-store/<key>, method=GET
URL_for: kvs_get, key=?
Purpose: 1) Get the value of a key in the store     
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store/<key>', methods=['GET'])
def kvs_get(key):
    # GET request return the corresponding value of the key
    new_shard_id = hash_a_string(key) % shard_count + 1 
    # check if this is the "correct" shard
    if new_shard_id == my_shard:
        pass
    # else, forward the request to a "correct" shard member
    else:
        forwarding_ip = kvs_first_member(new_shard_id)
        try:
            forw = requests.get(request.url.replace(request.host_url, 'http://' + str(forwarding_ip) + '/' ), timeout=1)
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            return Response(status=404) 
        else:
            return Response(forw.content, forw.status_code)

    value = None
    # loop through history to find the latest value of the key
    for item in history:
        if item[1] == key:
            version = item[0]
            value = item[2]
            cm = item[3]

    if value is not None:
        resp = jsonify(**{'message':'Retrieved successfully', 'version':version, 'causal-metadata':cm, 'value':value})
        resp.statue_code = 200
        return resp
    else:
        return bad_request('GET')

############################################################################################################

######################################## KVS HELPERS #######################################################
# function for put/delete operation from replica
# return response
# result = {'version': version, 'causal-metadata': cm, 'value': value}
def op_from_replica(request, key, result) -> Response:
    print("this is from another replica, do not broadcast")
    version = result['version'] # EX: "key?10.10.0.3:8083:2"
    cm = result['causal-metadata']
    value = result['value']
    updated_cm = version if cm is '' else cm + version
    resp = jsonify(message="Replicated successfully", version=version)
    resp.status_code = 201
    add_element_to_history(version, key, value, updated_cm)
    return resp

# generate a new version based on current counter + this ip
# return a unique version across replicas ex: "10.10.0.3:8080:0"
def generate_version(key) -> str:
    global counter
    # generate new version
    version = str(key) + '?' + str(this_ip) + ':' + str(counter)
    counter += 1
    return version

# create an element based on received data and add it to history
def add_element_to_history(version, key, value, cm) -> None:
    global history
    history_element = (version, key, value, cm) # ( version, key, value, causal_metadata)
    history.append(history_element)

# return a list of all members in the shard
def kvs_shard_members(shard_id) -> list:
    global running_ip
    global my_shard
    global this_ip
    members = []

    # check shard_id of all nodes
    for ip in running_ip:
        if ip is not this_ip:
            try:
                resp = requests.get('http://' + str(ip) + '/key-value-store-shard/node-shard-id')
                result = int(resp.json().get('shard-id'))
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('shard-id-members error')
            if result == shard_id:
                members.append(ip)

    return members

# return the first member in a shard
def kvs_first_member(shard_id) -> str:
    global running_ip
    global this_ip
    member = None

    for ip in running_ip:
        if ip is not this_ip:
            try:
                resp = requests.get('http://' + str(ip) + '/key-value-store-shard/node-shard-id')
                result = int(resp.json().get('shard-id'))
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('kvs_first_member error')
            if result == shard_id:
                member = ip
                break

    return member


# braodcast request to other replicas
# log response status code to console
def broadcast_request(request, forwarding_data, members) -> None:
    # broadcast request to other replicas in shard
    for ip in members:
        if ip != this_ip:
            try:

                resp = requests.request(
                    method=request.method,
                    url=request.url.replace(request.host_url, 'http://' + str(ip) + '/' ),
                    json=forwarding_data,
                    timeout=2
                )
                response = Response(resp.content, resp.status_code)
            except (requests.Timeout) as e:
                pass 

# return a hash int of the key string
def hash_a_string(st) -> int:
    return int(hashlib.sha256(st.encode('utf-8')).hexdigest(), 16) % 10**8

############################################################################################################

######################################## VIEW OPERATIONS ###################################################
"""
Endpoint: /key-value-store-view, method=GET, PUT, DELETE
URL_for: view
Purpose: 1) Get, put, or delete the view of a running ip  
Accessed by: Client or any nodes  
"""
@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def view():
    global ping_leader
    # GET request retrieve the view from another replica
    if request.method == 'GET':
        return view_get_method()

    # DELETE reqeust delete a replica from the view
    if request.method == 'DELETE':
        res = request.get_json(force=True)
        delete_ip = res['socket-address']
        if delete_ip == ping_leader:
            ping_leader = request.remote_addr + ":8080"
        return view_delete_method(delete_ip)

    # PUT reqeust add a new replica to the view
    if request.method == 'PUT':
        res = request.get_json(force=True)
        new_ip = res['socket-address']
        return view_put_method(new_ip)

# Helper function for GET
def view_get_method():
    view = ','.join(running_ip)
    message = jsonify(message='View retrieved successfully', view=view)
    resp = make_response(message, 200)
    return resp

# Helper function for DELETE
def view_delete_method(bad_ip):
    global running_ip
    if bad_ip in running_ip:
        running_ip.remove(bad_ip)
        message = jsonify(message='Replica deleted successfully from the view')
        resp = make_response(message, 200)
        return resp
    else:
        # DELETE error
        return bad_view_request('DELETE')

# Helper function for PUT
def view_put_method(new_ip):
    global running_ip
    if new_ip not in running_ip:
        running_ip.append(new_ip)
        message = jsonify(message='Replica added successfully to the view')
        resp = make_response(message, 201)
        return resp
    else:
        # PUT error
        return bad_view_request('PUT')

#####################################################################################################

########################################## MAIN #####################################################
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080, threaded=True)
#####################################################################################################