import unittest 
import requests
import os 
import time

######################## initialize variables ################################################
subnetName = "my-net"
subnetAddress = "10.10.0.0/16"

nodeIpList = ["10.10.0.2", "10.10.0.3", "10.10.0.4", "10.10.0.5", "10.10.0.6", "10.10.0.7"]
nodeHostPortList = ["8082","8083", "8084", "8085", "8086", "8087"]
nodeSocketAddressList = [ replicaIp + ":8080" for replicaIp in nodeIpList ]

view = ""
for nodeSocketAddress in nodeSocketAddressList:
    view += nodeSocketAddress + ","
view = view[:-1]

shardCount = 2

############################### Docker Linux Commands ###########################################################
def removeSubnet(subnetName):
    command = "docker network rm " + subnetName
    os.system(command)
    time.sleep(2)

def createSubnet(subnetAddress, subnetName):
    command  = "docker network create --subnet=" + subnetAddress + " " + subnetName
    os.system(command)
    time.sleep(2)

def buildDockerImage():
    command = "docker build -t kvs-img ."
    os.system(command)

def runInstance(hostPort, ipAddress, subnetName, instanceName):
    command = "docker run -d -p " + hostPort + ":8080 --net=" + subnetName + " --ip=" + ipAddress + " --name=" + instanceName + " -e SOCKET_ADDRESS=" + ipAddress + ":8080" + " -e VIEW=" + view + " -e SHARD_COUNT=" + str(shardCount) + " kvs-img"
    os.system(command)
    time.sleep(2)

def runAdditionalInstance(hostPort, ipAddress, subnetName, instanceName, newView):
    command = "docker run -d -p " + hostPort + ":8080 --net=" + subnetName + " --ip=" + ipAddress + " --name=" + instanceName + " -e SOCKET_ADDRESS=" + ipAddress + ":8080" + " -e VIEW=" + newView  + " kvs-img"
    os.system(command)
    time.sleep(20)

def stopAndRemoveInstance(instanceName):
    stopCommand = "docker stop " + instanceName
    removeCommand = "docker rm " + instanceName
    os.system(stopCommand)
    time.sleep(2)
    os.system(removeCommand)

def connectToNetwork(subnetName, instanceName):
    command = "docker network connect " + subnetName + " " + instanceName
    os.system(command)

def disconnectFromNetwork(subnetName, instanceName):
    command = "docker network disconnect " + subnetName + " " + instanceName
    os.system(command)

################################# Unit Test Class ############################################################

class TestKVS(unittest.TestCase):

    shardIdList = []
    shardsMemberList = []
    keyCount = 100

    ######################## Build docker image and create subnet ################################
    print("###################### Building Docker Image ######################\n")
    # build docker image
    buildDockerImage()

    # stop and remove containers from possible previous runs
    print("\n###################### Stopping and removing containers from previous run ######################\n")
    stopAndRemoveInstance("node1")
    stopAndRemoveInstance("node2")
    stopAndRemoveInstance("node3")
    stopAndRemoveInstance("node4")
    stopAndRemoveInstance("node5")
    stopAndRemoveInstance("node6")
    stopAndRemoveInstance("node7")

    print("\n###################### Creating the subnet ######################\n")
    # remove the subnet possibly created from the previous run
    removeSubnet(subnetName)

    # create subnet
    createSubnet(subnetAddress, subnetName)

    print("\n###################### Running Tests ######################\n")

     # run instances
    print("\n###################### Running Instances ######################\n")
    runInstance(nodeHostPortList[0], nodeIpList[0], subnetName, "node1")
    runInstance(nodeHostPortList[1], nodeIpList[1], subnetName, "node2")
    runInstance(nodeHostPortList[2], nodeIpList[2], subnetName, "node3")
    runInstance(nodeHostPortList[3], nodeIpList[3], subnetName, "node4")
    runInstance(nodeHostPortList[4], nodeIpList[4], subnetName, "node5")
    runInstance(nodeHostPortList[5], nodeIpList[5], subnetName, "node6")

    time.sleep(10)

    ########################## Run tests #######################################################

    ########################## View tests #######################################################
    def test_a_view_operations(self):

        print("\n###################### Getting the view from nodes ######################\n")
        # get the view from node1
        response = requests.get( 'http://localhost:8082/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from node2
        response = requests.get( 'http://localhost:8083/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from node3
        response = requests.get( 'http://localhost:8084/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from node4
        response = requests.get( 'http://localhost:8085/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from node5
        response = requests.get( 'http://localhost:8086/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from node6
        response = requests.get( 'http://localhost:8087/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        print("\n###################### Putting new node into the view ######################\n")
        node7Ip = "10.10.0.8"
        node7HostPort = "8088"
        node7SocketAddress = "10.10.0.8:8080"
        newView = view + "," + node7SocketAddress

        runAdditionalInstance(node7HostPort, node7Ip, subnetName, "node7", newView)

        time.sleep(5)

        # get the new view from node1
        response = requests.get( 'http://localhost:8082/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], newView)

        # get the new view form node2
        response = requests.get( 'http://localhost:8083/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], newView)

        # get the view from node3
        response = requests.get( 'http://localhost:8084/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], newView)

        print("\n###################### Deleting the new node from the view ######################\n")
        # stop and remove the new container
        stopAndRemoveInstance("node7")
        time.sleep(50)

        # get the view from node4
        response = requests.get( 'http://localhost:8085/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from node5
        response = requests.get( 'http://localhost:8086/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from node6
        response = requests.get( 'http://localhost:8087/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

    # def test_b_ping_operations(self):

    ########################## Shard tests #######################################################
    def test_c_get_shard_ids(self):
        time.sleep(10)

        print("\n###################### Getting Shard IDs ######################\n")

        # get the shard IDs from node1
        response = requests.get( 'http://localhost:8082/key-value-store-shard/shard-ids')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        shardIdsFromNode1 = responseInJson['shard-ids']
        self.assertEqual(len(shardIdsFromNode1.split(",")), shardCount)

        # get the shard IDs from node5
        response = requests.get( 'http://localhost:8086/key-value-store-shard/shard-ids')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        shardIdsFromNode5 = responseInJson['shard-ids']
        self.assertEqual(shardIdsFromNode5, shardIdsFromNode1)

        # get the shard IDs from node6
        response = requests.get( 'http://localhost:8087/key-value-store-shard/shard-ids')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        shardIdsFromNode6 = responseInJson['shard-ids']
        self.assertEqual(shardIdsFromNode6, shardIdsFromNode1)

        self.shardIdList += shardIdsFromNode1.split(",")
    
    def test_d_shard_id_members(self):

        print("\n###################### Getting the Members of Shard IDs ######################\n")

        shard1 = self.shardIdList[0]
        shard2 = self.shardIdList[1]

        # get the members of shard1 from node2
        response = requests.get( 'http://localhost:8083/key-value-store-shard/shard-id-members/' + shard1)
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        shard1Members = responseInJson['shard-id-members']
        self.assertGreater(len(shard1Members.split(",")), 1)

        # get the members of shard2 from node3
        response = requests.get( 'http://localhost:8084/key-value-store-shard/shard-id-members/' + shard2)
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        shard2Members = responseInJson['shard-id-members']
        self.assertGreater(len(shard2Members.split(",")), 1)

        self.assertEqual(len(nodeSocketAddressList), len(shard1Members.split(",") + shard2Members.split(",")))

        self.shardsMemberList += [shard1Members]
        self.shardsMemberList += [shard2Members]
    
    
    def test_e_node_shard_id(self):
    
        print("\n###################### Getting the Shard ID of the nodes ######################\n")
    
        shard1 = self.shardIdList[0]

        # get the shard id of node1
        response = requests.get( 'http://localhost:8082/key-value-store-shard/node-shard-id')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)

        node1ShardId = responseInJson['shard-id']

        self.assertTrue(node1ShardId in self.shardIdList)

        if node1ShardId == shard1:
            self.assertTrue(nodeSocketAddressList[0] in self.shardsMemberList[0].split(","))
        else:
            self.assertTrue(nodeSocketAddressList[0] in self.shardsMemberList[1].split(","))

        # get the shard id of node2
        response = requests.get('http://localhost:8083/key-value-store-shard/node-shard-id')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)

        node2ShardId = responseInJson['shard-id']

        self.assertTrue(node2ShardId in self.shardIdList)

        if node2ShardId == shard1:
            self.assertTrue(nodeSocketAddressList[1] in self.shardsMemberList[0].split(","))
        else:
            self.assertTrue(nodeSocketAddressList[1] in self.shardsMemberList[1].split(","))

        # get the shard id of node6
        response = requests.get('http://localhost:8087/key-value-store-shard/node-shard-id')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)

        node6ShardId = responseInJson['shard-id']

        self.assertTrue(node6ShardId in self.shardIdList)

        if node6ShardId == shard1:
            self.assertTrue(nodeSocketAddressList[5] in self.shardsMemberList[0].split(","))
        else:
            self.assertTrue(nodeSocketAddressList[5] in self.shardsMemberList[1].split(","))
    

if __name__ == '__main__':
    unittest.main() 