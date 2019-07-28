# myapp.py
# CS 128 Assignment 4
# Sharded, Fault Tolerant Key-Value store by replication that provides causal consistency
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


# ---------------------- Declare Variables ----------------------
running_ip =[] # store view of this replica
this_ip = None  # store self socket address
history = []  # EX: (key?10.10.0.3:1, mykey, myval, causal-metadata)
pending_request = []
counter = 0
shard_count = 0 # store external shard_count
my_shard = 0 # store self shard id


# ------------------------ Error Handlers --------------------------
# View error for PUT and DELETE
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
    if e is 'NO':
        resp = jsonify(message='You do not have the right to access this domain')
    resp.status_code = 400
    return resp


# ----------------------- App Initialization -----------------------
def check_view_list():
    app = Flask(__name__)
    # Need to set global keyword to avoid ambiguity
    global this_ip
    global running_ip
    global history
    global shard_count
    global my_shard

    # get external view and socket_address data
    running_ip = os.environ.get('VIEW').split(',') # a list of string of running ip, including itself
    this_ip = str(os.environ.get('SOCKET_ADDRESS')) # get self ip address

    # get external shard count
    # check if SHARD_COUNT is provided
    if os.environ.get('SHARD_COUNT') is not None:
        shard_count = int(os.environ.get('SHARD_COUNT'))
        if ((shard_count*2) <= (len(running_ip))):
            # assign shard id to node using position mod shard_count
            my_shard = ((running_ip.index(this_ip) + 1) % shard_count) + 1

    # if SHARD_COUNT is not provided, need to add to a shard use add-member

    # do a PUT broadcast to every other running ip in it's view
    def view_put_broadcast():
        for ip in running_ip:
            if ip != this_ip:
                try:
                    socket_add = {'socket-address': this_ip}
                    resp = requests.request(
                        method='PUT',
                        url='http://' + str(ip) + '/key-value-store-view',
                        json=socket_add
                    )
                    response = Response(resp.content, resp.status_code)
                    
                except (requests.Timeout, requests.exceptions.RequestException) as e:
                    print("cannot send put view request to a bad ip")
                    

    thread = threading.Thread(target=view_put_broadcast)
    thread.start()

    # retrive all key-value pairs from one of the replicas and put them into local store
    if len(running_ip) > 1:
        global history
        random_ip = None

        # do not retrive history if shard id is unkown
        if my_shard == 0:
            print('Do not retrive history')

        else:
            for ip in running_ip:
                if ip is not this_ip:
                    try:
                        rand = requests.request(
                            method = 'GET',
                            url = 'http://' + str(ip) + '/key-value-store-shard/node-shard-id'
                        )
                        result_id = int(rand.json().get('shard-id'))
                    except (requests.Timeout, requests.exceptions.RequestException) as e:
                        print('retrive history node-shard-id error')
                    else:
                        if result_id == my_shard:
                            random_ip = ip
                            break

            print("random_ip" + str(random_ip))
            try:
                resp = requests.request(
                    method='GET',
                    url='http://' + str(random_ip) + '/history'
                )
                history = resp.json().get('history')
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print("error")

    return app

app = check_view_list()
executor = Executor(app)


# ------------------------- SHARD OPERATIONS -------------------------------------
# return all shard ids of the store
@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def shard_ids():
    return return_shard_ids()

# return shard id of a node
@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def node_shard_id():
    return return_node_shard_id()

# return all members of a shard id, return list of members' socket address
@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=['GET'])
def shard_members(shard_id):
    global running_ip
    global my_shard
    global this_ip
    members = []
    shard_id = int(shard_id)

    # check shard_id of all nodes
    for ip in running_ip:
        if ip is not this_ip:
            try:
                resp = requests.request(
                    method = 'GET',
                    url = 'http://' + str(ip) + '/key-value-store-shard/node-shard-id',
                )
                result = int(resp.json().get('shard-id'))
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('shard-id-members error')
            if result == shard_id:
                members.append(ip)
        elif ip is this_ip:
            if my_shard == shard_id:
                members.append(ip)

    return return_shard_members(members)

# return number of keys stored in a shard
@app.route('/key-value-store-shard/shard-id-key-count/<shard_id>', methods=['GET'])
def shard_id_key_count(shard_id):
    global history
    global my_shard
    global running_ip
    global this_ip
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
                    resp = requests.request(
                        method = 'GET',
                        url = 'http://' + str(ip) + '/key-value-store-shard/node-shard-id'
                    )
                    result = int(resp.json().get('shard-id'))
                except (requests.Timeout, requests.exceptions.RequestException) as e:
                    print('shard-id-members error')
                if result == int(shard_id):
                    forward_ip = ip
                    break
        forw = requests.request(
            method = 'GET',
            url = request.url.replace(request.host_url, 'http://' + str(forward_ip) + '/')
        )
        return Response(forw.content, forw.status_code)

# add a node to a shard
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
    if new_add == this_ip:
        # assign shard id
        my_shard = shard_id
        # get shard count data from a random ip
        for ip in running_ip:
            if ip is not this_ip:
                random_ip = ip
                break
        try:
            resp = requests.request(
                method = 'GET',
                url = 'http://' + str(random_ip) + '/key-value-store-shard/shard-ids'
            )
            shard_ids = resp.json().get('shard-ids').split(',')
            shard_count = int(shard_ids[-1])
        except (requests.Timeout, requests.exceptions.RequestException) as e:
            print("Error in add member get shard count")

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

@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard_keys():
    result = request.get_json(force=True) 
    reshard_number = int(result['shard-count']) 
    # check if there's sufficient replica to do the requested number of shards
    # there has to be at least two replicas in a shard
    if len(running_ip)  < reshard_number * 2:
        return bad_request('SHARD')
    elif reshard_number == shard_count:
        resp = jsonify(message='Client, this is not a new shard count : ) Shy shy shy Fancy woo') 
        resp.status_code = 200
        return resp 
    
    # run the reshard process concurrently in the background task queue
    executor.submit(reshard, reshard_number) 

    # return response cuz the client is waitingggg
    time.sleep(2)
    resp = jsonify(message='Resharding done successfully')
    resp.status_code = 200
    return resp

# the function for overriding a node's kvs, shard id, and shard count knowledge 
@app.route('/key-value-store-shard/reshard_history', methods=['PUT'])
def reshard_keys_from_replica():
    global history 
    global my_shard
    global shard_count
    print("herroooo1")
    request_source = request.remote_addr + ":8080"
    # security check, if this request is not from another node, it can be dangerous 
    if request_source not in running_ip:
        return bad_request('NO') 
    
    print("herroooo2")
    # get a bunch of data
    result = request.get_json(force=True)
    new_history = result['new_history']
    new_id = result['new_id']
    new_count = result['new_count']
    # override these data 
    history = new_history
    my_shard = int(new_id)
    shard_count = int(new_count)
    resp = jsonify(message="Successfully")
    resp.status_code = 201
    return resp

# ----------------------------- Helper Functions for Sharding --------------------------------
def return_shard_ids():
    global shard_count
    shards = []
    for i in range(1, (shard_count+1)):
        shards.append(i)
    shard_ids = ','.join([str(shard) for shard in shards])
    message = jsonify(**{'message':'Shard IDs retrieved successfully', 'shard-ids':shard_ids})
    resp = make_response(message, 200)
    return resp

def return_node_shard_id():
    global my_shard
    shard_id = str(my_shard)
    message = jsonify(**{'message':'Shard ID of the node retrieved successfully', 'shard-id':shard_id})
    resp = make_response(message, 200)
    return resp

def return_shard_members(members):
    shard_id_members = ','.join(members)
    message = jsonify(**{'message':'Members of shard ID retrieved successfully', 'shard-id-members':shard_id_members})
    resp = make_response(message, 200)
    return resp

def return_key_count(count):
    key_count = str(count)
    message = jsonify(**{'message':'Key count of shard ID retrieved successfully', 'shard-id-key-count':key_count})
    resp = make_response(message, 200)
    return resp

def return_add_member(shard_id):
    global history

    # select a node within the same shard id
    get_add = None
    for ip in running_ip:
        if ip is not this_ip:
            try:
                resp = requests.request(
                    method = 'GET',
                    url = 'http://' + str(ip) + '/key-value-store-shard/node-shard-id'
                )
                node_shard_id = int(resp.json().get('shard-id'))
                if node_shard_id == shard_id:
                    get_add = ip
                    break
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('error in return add member')
    
    # retrive history data from this node
    try:
        resp = requests.request(
            method = 'GET',
            url = 'http://' + str(get_add) + '/history'
        )
        history = resp.json().get('history')
    except (requests.Timeout, requests.exceptions.RequestException) as e:
        print('error in return add member retrive history')

    # return message
    message = jsonify(message='Add member successfully')
    resp = make_response(message, 200)
    return resp

# function for resharding 
def reshard(reshard_number):
    global history 
    global shard_count
    global my_shard
    global running_ip
    global this_ip
    # initialize a list for all kvs across shards, with its own list of shard history as default
    all_kvs = history 
    print("all_kvs", all_kvs)
    # initialize a list for all shard ids, also has its own id as default
    all_ids = [my_shard] 
    print("I'm inside resharddddd")
    # search through all the nodes
    print('running_ip', running_ip)
    for ip in running_ip:
        print("in reshard" , ip)
        if this_ip != ip:
            # get the shard id of this ip 
            try: 
                resp = requests.request(
                        method='GET',
                        url='http://' + str(ip) + '/key-value-store-shard/node-shard-id'
                    )
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('reshard error')
            else:
                this_shard_id = int(resp.json().get('shard-id'))
                print("this_shard_id", this_shard_id)
                if this_shard_id not in all_ids:
                    all_ids.append(this_shard_id)
                    try:
                        resp = requests.request(
                            method='GET',
                            url='http://' + str(ip) + '/history'
                        )

                        history_from_ip = resp.json().get('history') 

                        all_kvs = all_kvs + history_from_ip

                        if len(all_ids) == shard_count:
                            break
                    except (requests.exceptions.RequestException) as e:
                        print("Get history timeout in reshard")
    # # initialize a new dict for resharding kvs
    data = {i: [] for i in range(1, reshard_number+1)} 
    for item in all_kvs:
        print("why is it not going to this line??")
        print("item", item)
        print("item[1]", item[1])
        # find the key's new corresponding shard id 
        new_shard_id = ( hash_a_string(item[1]) % reshard_number) + 1
        # add the item to the dict list for it 
        data[new_shard_id].append(item)
    
    print("here is reshard dict", data) 
    
    count = 1
    for ip in running_ip:
        ip_new_shard_id = count
        print("count of shard num", count)
        count = count  % reshard_number + 1 
        if ip != this_ip:
            # evenly assign new shard id for each node, to ensure each shard has at least two nodes 

            forwarding_data = {'new_history': data[ip_new_shard_id], 
                               'new_id': str(ip_new_shard_id),
                               'new_count': str(reshard_number)} 
            # send request to this ip to put the new history, shard count, and shard id into it, override the old 
            try:
                resp = requests.request(
                    method='PUT',
                    url='http://' + str(ip) + '/key-value-store-shard/reshard_history',
                    json=forwarding_data
                )
                print(resp.status_code)
                print("hey there sexy")
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print("Request timeout in reshard")
        else:
            my_shard = ip_new_shard_id
            shard_count = reshard_number
            history = data[ip_new_shard_id] 

    return 'success'


# -------------------------- KVS OPERATIONS -----------------------------------------
# The MAIN endpoint for key value store operation
@app.route('/key-value-store/<key>', methods=['PUT', 'GET', 'DELETE'])
def api_kvs(key):
    global counter
    global shard_count
    global my_shard
    global running_ip
    global this_ip

    #--------------------------key hashing-------------------------------------------
    new_shard_id = hash_a_string(key) % shard_count + 1 
    
    print("This is hash of key", hash_a_string(key))
    print("From node", this_ip)
    #-----------------------end key hashing--------------------------------------------

    # PUT request add or update key
    if request.method == 'PUT':
        # get request ip, to check if it's from client
        request_source = request.remote_addr + ":8080"
        # ---------------------------get the data passed in-----------------------------
        result = request.get_json(force=True)
        value = result['value']
        cm = result['causal-metadata'] # EX: "key1?10.10.0.2:1,key2?10.10.0.2:2"

        # if this is the "correct" shard, do request normal
        if new_shard_id == int(my_shard):
            # When the dependencies are not satisfied, wait
            # Flask can process request concurrently by default
            # Need to check for causal consistency if there are causal-metadata passed in
            if cm != "":
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
                        resp = requests.request(
                            method = 'GET',
                            url = 'http://' + str(forwarding_ip) + '/history'
                        )
                        forward_history = resp.json().get('history')
                        while item not in [his[0] for his in forward_history]:
                            time.sleep(1)
                            forwarding_ip = kvs_first_member(cm_shard)
                            resp = requests.request(
                                method = 'GET',
                                url = 'http://' + str(forwarding_ip) + '/history'
                            )
                            forward_history = resp.json().get('history')
                        
            # if request from client
            if request_source not in running_ip:
                print("this is from client")
                return put_op_from_client(request, key, value, cm, new_shard_id)
            # if request id forwarded from another node
            elif 'from-shard' in result:
                return put_op_from_client(request, key, value, cm, new_shard_id)
            else:
                return op_from_replica(request, key, result)

        # else, forward the request to the "correct" shard member
        else:
            forwarding_ip = kvs_first_member(new_shard_id)
            forwarding_data = {'value': value, 'causal-matadata': cm, 'from-shard': 1}
            forw = requests.request(
                method = request.method,
                url = request.url.replace(request.host_url, 'http://' + str(forwarding_ip) + '/'),
                json = forwarding_data
            )
            return Response(forw.content, forw.status_code)

    # GET request return the corresponding value of the key
    if request.method == 'GET':
        # check if this is the "correct" shard
        if new_shard_id == my_shard:
            print("goAhead")
        # else, forward the request to a "correct" shard member
        else:
            forwarding_ip = kvs_first_member(new_shard_id)
            forw = requests.request(
                method = request.method,
                url=request.url.replace(request.host_url, 'http://' + str(forwarding_ip) + '/' )
            )
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

    # DELETE request delete the corresponding key-value pair
    # if request.method == 'DELETE':
    #     # get request ip, to check if it's from client
    #     request_source = request.remote_addr + ":8080"
    #     # ---------------------------get the data passed in-----------------------------
    #     result = request.get_json(force=True)
    #     value = None
    #     cm = result['causal-metadata']
    #     cm_list = cm.split(',') if cm != '' else ''
    #     # ---------------------------get the data passed in-----------------------------

    #     # while not all(item in [ item[0] for item in history ] for item in cm_list):
    #     #     time.sleep(1)

    #     # if this is the "correct" shard, do request normal
    #     if new_shard_id == int(my_shard):
    #         # loop through history to find the latest value of the key
    #         for item in history:
    #             if item[1] == key:
    #                 value = item[2]

    #         # if the last item that correspond to the key is not None
    #         if value != None:
    #             # if the request is from client
    #             if request_source not in running_ip:
    #                 return delete_op_from_client(request, key, result, cm, new_shard_id)
    #             # if the request is forwarded from another node
    #             elif 'from-shard' in result:
    #                 return delete_op_from_client(request, key, result, cm, new_shard_id)
    #             else:
    #                 return op_from_replica(request, key, result, cm)
    #         return bad_request('DELETE')

    #     # else, forward the request to the "correct" shard member
    #     else:
    #         forwarding_ip = kvs_first_member(new_shard_id)
    #         forwarding_data = {'causal-metadata': cm, 'from-shard': 1}
    #         forw = requests.request(
    #             method = request.method,
    #             url = request.url.replace(request.host_url, 'http://' + str(forwarding_ip) + '/'),
    #             json = forwarding_data
    #         )
    #         return Response(forw.content, forw.status_code)


# -------------------------- KVS HELPER FUNCTIONS -----------------------------------------

# function for deleting put operation from client
# return response
def put_op_from_client(request, key, value, cm, shard_id):
    global my_shard
    print("I'm gonna broadcast!!")
    version = generate_version()
    version = key + '?' + version
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

# function for deleting key operation from client
# return response
# def delete_op_from_client(request, key, result, cm, shard_id):
#     print("I'm gonna broadcast!!")
#     value = None
#     version = generate_version()
#     forwarding_data = {'version': version, 'causal-metadata': cm, 'value': value }
#     executor.submit(broadcast_request, request, forwarding_data)
#     # expand the causal metadata containing coresponding key to the version
#     updated_cm = key + '-' + version if cm is '' else (cm + ',' + key + '-' + version)
#     shard_id = str(shard_id)
#     resp = jsonify(**{'message':'Deleted successfully', 'version':version, 'causal-metadata':updated_cm, 'shard-id': shard_id})
#     resp.status_code = 200
#     add_element_to_history( version, key, value, updated_cm )
#     return resp

    # function for put/delete operation from replica
    # return response
    # result = {'version': version, 'causal-metadata': cm, 'value': value}
    def op_from_replica(request, key, result):
        print("this is from another replica, do not broadcast")
        version = result['version'] # EX: "key?10.10.0.3:2"
        cm = result['causal-metadata']
        value = result['value']
        updated_cm = versiont if cm is '' else cm + version
        resp = jsonify(message="Replicated successfully", version=version)
        resp.status_code = 201
        add_element_to_history(version, key, value, updated_cm)
        return resp

# generate a new version based on current counter + this ip
# return a unique version across replicas ex: "10.10.0.3:8080:0"
def generate_version():
    global counter
    # generate new version
    version = str(this_ip) + ':' + str(counter)
    counter += 1
    return version

# create an element based on received data and add it to history
def add_element_to_history(version, key, value, cm):
    global history
    history_element = (version, key, value, cm) # ( version, key, value, causal_metadata)
    history.append(history_element)

# return a list of all members in the shard
def kvs_shard_members(shard_id):
    global running_ip
    global my_shard
    global this_ip
    members = []

    # check shard_id of all nodes
    for ip in running_ip:
        if ip is not this_ip:
            try:
                resp = requests.request(
                    method = 'GET',
                    url = 'http://' + str(ip) + '/key-value-store-shard/node-shard-id',
                )
                result = int(resp.json().get('shard-id'))
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('shard-id-members error')
            if result == shard_id:
                members.append(ip)

    return members

# return the first member in a shard
def kvs_first_member(shard_id):
    global running_ip
    global this_ip
    member = None

    for ip in running_ip:
        if ip is not this_ip:
            try:
                resp = requests.request(
                    method = 'GET',
                    url = 'http://' + str(ip) + '/key-value-store-shard/node-shard-id'
                )
                result = int(resp.json().get('shard-id'))
            except (requests.Timeout, requests.exceptions.RequestException) as e:
                print('kvs_first_member error')
            if result == shard_id:
                member = ip
                break

    return member


# braodcast request to other replicas
# log response status code to console
def broadcast_request(request, forwarding_data, members):
    #-----------------------------------------------BROADCAST-------------------------------------------------------------
    # broadcast PUT request to other replicas in shard
    print("members", members) 
    for ip in members:
        if ip != this_ip:
            try:
                resp = requests.request(
                    method=request.method,
                    url=request.url.replace(request.host_url, 'http://' + str(ip) + '/' ),
                    json=forwarding_data,
                    timeout=5
                )
                response = Response(resp.content, resp.status_code)

            # delete not responding replicas
            except (requests.Timeout) as e:
                running_ip.remove(ip)
                # def view_delete_broadcast():
                for ipp in running_ip:
                    if ipp!=ip and ipp!=this_ip:
                        socket_delete = {'socket-address': str(ip)}
                        resp = requests.request(
                            method='DELETE',
                            url='http://' + str(ipp) + '/key-value-store-view',
                            json=socket_delete
                        )
                        response = Response(resp.content, resp.status_code)
                        return response.status_code
                thread = threading.Thread(target=view_delete_broadcast)
                thread.start()

# -------------------------- END OF KVS HELPER FUNCTIONS -----------------------------------------

# -------------------------- VIEW OPERATIONS -----------------------------------------
# The MAIN endpoint for view operation
@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def view():
    # GET request retrieve the view from another replica
    if request.method == 'GET':
        return view_get_method()

    # DELETE reqeust delete a replica from the view
    if request.method == 'DELETE':
        res = request.get_json(force=True)
        delete_ip = res['socket-address']
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
# -------------------------- END OF VIEW OPERATIONS -----------------------------------------

def hash_a_string(st):
    return int(hashlib.sha256(st.encode('utf-8')).hexdigest(), 16) % 10**8


# --------------------------------- Retrive Data for New Replica ----------------------------------
@app.route('/history', methods=['GET'])
def get_kvs():
    resp = jsonify(message='Retreive key value store', history=history)
    resp.status_code = 200
    return resp

# --------------------------------- END of Retrive Data for New Replica ----------------------------------


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080, threaded=True)
