from flask import Flask, request, jsonify, json
from flask_api import status
app = Flask(__name__)
import os
import requests as req
import time
theDict = dict()
IPPORT = os.getenv('IPPORT')
K = int(os.getenv('K'))
timestamp = 0
partitionList = list()
if 'VIEW' in os.environ:
	ip_view = os.getenv('VIEW').split(',')
else:
	ip_view = list()
causal_payload = dict()
if len(ip_view) > 0:
	for ip in ip_view:
		causal_payload[ip] = 0



@app.route("/kvs/get_partition_id")
def getPartitionID():
	indexIP = ip_view.index(IPPORT)
	partitionID = int(indexIP/K)
	retDict = {"msg":"success","partition_id":partitionID}
	return jsonify(retDict), status.HTTP_200_OK

@app.route("/kvs/get_all_partition_ids")
def getPartitionIDList():
	theLen = int(len(ip_view))
	partitions = int((theLen-1)/K)+1
	retList = list()
	for i in range(0,partitions):
		retList.append(i)
	retDict = {"msg":"success","partition_id_list":retList}
	return jsonify(retDict), status.HTTP_200_OK


@app.route("/kvs/get_partition_members")
def getAllPartitionIDs():
	partitionID = int(request.form.get("partition_id"))
	partitionNodes = list()
	startIndex = partitionID * K
	endIndex = startIndex + K
	if int(len(ip_view)) < endIndex:
		endIndex = int(len(ip_view))
	for i in range(startIndex, endIndex):
		partitionNodes.append(ip_view[i])
	retDict = {"msg":"success","partition_members":partitionNodes}
	return jsonify(retDict), status.HTTP_200_OK
	


@app.route("/kvs/update_view", methods=["PUT"])
def view_update():
	node = request.form.get("ip_port")
	type = request.args.get("type")
	timeStamp = time.clock()
	hashView = list()
	for ip in ip_view:
		hashView.append(ip)
	updateIndex = -1
	getRemovedDict = False
	if type == "add":
		if node not in hashView:
			hashView.append(node)
			updateIndex = hashView.index(node)
	elif type == "remove":
		if node in hashView:
			delIndex = hashView.index(node)
			viewLastInd = int(len(hashView)) -1
			if delIndex != viewLastInd:
				hashView[delIndex], hashView[viewLastInd] = hashView[viewLastInd], hashView[delIndex]
			else:
				if viewLastInd%K==0:
					getRemovedDict = True
			hashView.remove(node)
			updateIndex = delIndex
			

	#Set all other ip_views to updated ip_view
	for server in ip_view: 
		if server != IPPORT: 
			if ping(server):
				req.put("http://" + server + "/kvs/removeView", timeout=1)
				for server2 in hashView:
					response = req.put("http://" + server + "/kvs/add/" + server2, timeout=1)

	#Update target node

	mustRehash = False
	theLen = int(len(hashView))
	partitions = int((theLen-1)/K)+1
	if partitions != int((len(ip_view)-1)/K)+1:
		mustRehash = True
	if type =="add":
		req.put("http://" + node + "/kvs/removeView", timeout=1)
		for server in hashView:
			response = req.put("http://" + node + "/kvs/add/" + server, timeout=1)

	
	thisPartition = int(hashView.index(IPPORT)/K)
	endIndex = (thisPartition+1)*K
	if endIndex > theLen:
		endIndex = theLen

	#following code block deals with various add/remove cases
	someDict = dict()
	if mustRehash:
		if type == "remove":
			if getRemovedDict:
				if not ping(node):
					retDict = {"error":"cannot connect to partition being removed to retrieve its dict before deletion"}
					return jsonify(retDict),  status.HTTP_503_UNAVAILABLE
				someRes = req.get("http://" + node + "/kvs/getDict", timeout=1)
				someDict = json.loads(someRes.content)
			elif updateIndex != -1:
				if not ping(hashView[updateIndex]):
					retDict = {"error":"cannot connect to partition being removed to retrieve its dict before deletion"}
					return jsonify(retDict),  status.HTTP_503_UNAVAILABLE
				someRes = req.get("http://" + hashView[updateIndex] + "/kvs/getDict", timeout=1)
				someDict = json.loads(someRes.content)
				someRes = req.get("http://" + hashView[updateIndex] + "/kvs/delDict", timeout=1)
			for key in someDict:
				theValue = someDict[key]
				nodeLocate = ryan_hash(key) % partitions
				endNode = (nodeLocate+1)*K
				if endNode > theLen:
					endNode = theLen
				if IPPORT not in hashView[nodeLocate*K:endNode]:
					rehashIP = getLiveIP(hashView[nodeLocate*K:endNode])
					if rehashIP == "ERROR":
						retDict = {"error":"Cannot connect to any node in partition: " + str(nodeLocate)}
						return jsonify(retDict), HTTP_503_UNAVAILABLE
					url_str = 'http://' + rehashIP + '/kvs/' + key
					someRes = req.put(url_str,data={'val':theValue}, timeout=6)
				else:
					theDict[key] = theValue
	else:
		if updateIndex != -1 and updateIndex < theLen:
			if hashView[updateIndex] not in hashView[thisPartition:endIndex]:
				updatePID = int(updateIndex/K)
				updateEnd = (updatePID+1)*K
				if updateEnd > theLen:
					updateEnd = theLen
				server = ""
				for ip in hashView[updatePID*K:updateEnd]:
					if ip != hashView[updateIndex]:
						if ping(ip):
							server = ip
							break
				someRes = req.get("http://" + ip + "/kvs/getDict", timeout=1)
				someDict = json.loads(someRes.content)	
				req.get("http://" + hashView[updateIndex] + "/kvs/delDict", timeout=1)			
				for key in someDict:
					theValue = someDict[key]
					url_str = 'http://' + hashView[updateIndex] + '/kvs/replicate/' + key
					someRes = req.put(url_str,data={'val':theValue}, timeout=6)
					
					
			


	#Rehash all other data
	if mustRehash:
		for i in range (0,partitions):
			server = ""
			canSend = False
			afterLast = (i+1)*K
			if afterLast > theLen:
				afterLast = theLen
			if IPPORT in hashView[i*K:afterLast]:
				continue
			server = getLiveIP(hashView[i*K:afterLast])
			if server == "ERROR":
				retDict = {"error":"Cannot connect to any node in partition: " + str(i)}
				return jsonify(retDict), HTTP_503_UNAVAILABLE
			someRes = req.get("http://" + server + "/kvs/getDict", timeout=1)
			someDict = json.loads(someRes.content)
			for ip in hashView[i*K:afterLast]:
				if ping(ip):
					req.get("http://" + ip + "/kvs/delDict", timeout=1)
			for key in someDict:
				theValue = someDict[key]
				nodeLocate = ryan_hash(key) % partitions
				endNode = (nodeLocate+1)*K
				if endNode > theLen:
					endNode = theLen
				if IPPORT not in hashView[nodeLocate*K:endNode]:
					rehashIP = getLiveIP(hashView[nodeLocate*K:endNode])
					if rehashIP == "ERROR":
						retDict = {"error":"Cannot connect to any node in partition: " + str(nodeLocate)}
						return jsonify(retDict), HTTP_503_UNAVAILABLE
					url_str = 'http://' + rehashIP + '/kvs/' + key
					someRes = req.put(url_str,data={'val':theValue}, timeout=6)
				else:
					theDict[key] = theValue
		#Rehash local data
		delList = list()
		for key in theDict:
			nodeLocate = ryan_hash(key) % partitions
			endNode = (nodeLocate+1)*K
			if endNode > theLen:
				endNode = theLen
			if IPPORT not in hashView[nodeLocate*K:endNode]:
				theValue = theDict[key]
				rehashIP = getLiveIP(hashView[nodeLocate*K:endNode])
				if rehashIP == "ERROR":
					retDict = {"error":"Cannot connect to any node in partition: " + str(nodeLocate)}
					return jsonify(retDict), HTTP_503_UNAVAILABLE
				url_str = "http://" + rehashIP + "/kvs/" + key
				req.put(url_str,data={'val':theValue}, timeout=5)
				delList.append(key)
		#Delete rehashed data that isn't wanted here
		for key in delList:
			del theDict[key]
	#Update replicas 
	for replica in hashView[thisPartition*K:endIndex]:
		if replica != IPPORT:
			if ping(ip):
				req.get("http://" + replica + "/kvs/delDict", timeout=1)
				for key in theDict:
					theValue = theDict[key]
					url_str = 'http://' + replica + '/kvs/replicate/' + key
					someRes = req.put(url_str,data={'val':theValue}, timeout=6)

	if type == "remove":
		if ping(node):
			someRes = req.get("http://" + node + "/kvs/delDict", timeout=5)
			someRes = req.put("http://" + node + "/kvs/removeView", timeout=5)


	del ip_view[:]
	for ip in hashView:
		ip_view.append(ip)

	timestamp = time.clock()
	if type == "add":
		thePID = int(updateIndex/K)
		retDict = {'msg':'success',"partition_id":thePID, "number_of_partitions":partitions}
	else:
		retDict = {'msg':'success','number_of_partitions':partitions}
	return jsonify(retDict), status.HTTP_200_OK



@app.route('/kvs/<key>', methods = ['GET', 'PUT']) 
def index(key):
	strClock = request.form.get('causal_payload')
	inClock = dict()
	if strClock != '':
		inClock = strClock

	theLen = int(len(ip_view))
	partitions = int((theLen-1)/K)+1
	nodeLocate = ryan_hash(key) % partitions
	startIndex = nodeLocate * K
	endIndex = (nodeLocate+1)*K
	if endIndex > int(len(ip_view)):
		endIndex = int(len(ip_view))
	if IPPORT in ip_view[startIndex:endIndex]:
		if request.method == 'PUT':				
			theValue = request.form.get('val')
			theDict[key] = theValue
			#For loop to forward put request to replicas
			for ip in ip_view[startIndex:endIndex]:
				if ip != IPPORT:
					if ping(ip):
						url_str = 'http://' + ip + '/kvs/replicate/' + key
						someRes = req.put(url_str,data={'val':theValue}, timeout=6)
			retDict = {"msg":"success","partition_id":nodeLocate,"causal_payload": causal_payload, "timestamp":timestamp}
			return jsonify(retDict), status.HTTP_201_CREATED	
		elif request.method == 'GET':
			#check if data is stale
			#For loop, compare vector clocks? If small vector clock here
			#overwrite data
			
			if key in theDict:
				theRet = theDict[key]
				retDict = {'msg':'success', 'value':theRet, 'partition_id':nodeLocate, 'causal_payload':causal_payload, 'timestamp':timestamp}
				#For loop to update stale datas
				return jsonify(retDict),status.HTTP_200_OK
			else:
				retDict = {'msg':'error', 'error':'key does not exist'}
				return jsonify(retDict),status.HTTP_404_NOT_FOUND
		else:
			return jsonify('Error: Please call a GET or PUT method.\n')
	else:
		if request.method == 'PUT':
			#For loop, ping nodes in clusters, PUT to first alive
			ip = getLiveIP(ip_view[startIndex:endIndex])
			if ip == "ERROR":
				retDict = {"error":"Cannot connect to any node in partition: " + str(nodeLocate)}
				return jsonify(retDict), HTTP_503_UNAVAILABLE
			theValue = request.form.get('val')
			url_str = 'http://' + ip + '/kvs/' + key
			resp = req.put(url_str,data={"val":theValue,"causal_payload":causal_payload})
			dRet = json.loads(resp.content)
			return jsonify(dRet), resp.status_code
		elif request.method == 'GET':
			#For loop, ping nodes in cluster, GET to first alive OR compare all live VC's
			ip = getLiveIP(ip_view[startIndex:endIndex])
			if ip == "ERROR":
				retDict = {"error":"Cannot connect to any node in partition: " + str(nodeLocate)}
				return jsonify(retDict), HTTP_503_UNAVAILABLE
			url_str = 'http://' + ip + '/kvs/' + key
			resp = req.get(url_str,data={"causal_payload":causal_payload})
			dRet = json.loads(resp.content)
			return jsonify(dRet), resp.status_code



@app.route("/kvs/add/<theIP>", methods=["PUT"])
def add(theIP):
	if theIP not in ip_view:
		ip_view.append(theIP)
		retDict = {'msg':'success'}
		return jsonify(retDict), status.HTTP_201_CREATED
	else:
		retDict = {'msg':'node already exists'}
		return jsonify(retDict), status.HTTP_200_OK

@app.route("/kvs/removeView", methods=["PUT"])
def remove():
	del ip_view[:]
	retDict = {'msg':'success'}
	return jsonify(retDict), status.HTTP_200_OK



@app.route("/kvs/getDict", methods=["GET"])
def getDict():
	return jsonify(theDict)

@app.route("/kvs/delDict", methods=["GET"])
def delDict():
	theDict = dict()
	someRet = getLiveIP(ip_view)
	return jsonify("del complete")

@app.route("/kvs/replicate/<key>", methods=["PUT"])
def replicatePut(key):
	theValue = request.form.get('val')
	theDict[key] = theValue
	return jsonify("replicated")


@app.route("/kvs/replaceClock", methods=["PUT"])
def replaceClock(key):
	theValue = request.form.get('clock')
	theDict[key] = theValue
	return jsonify("replicated")


@app.route("/kvs/getClock")
def getClock():
	return jsonify(causal_payload)




def greaterThan(clockA, clockB):
	for key in clockA:
		if key not in clockB:
			clockB[key] = 0
	for key in clockB:
		if key not in clockA:
			clockA[key] = 0
	isBigger = False
	for key in clockA:
		if clockA[key] < clockB[key]:
			return False
		if clockA[key]>clockB[key]:
			isBigger = True
	return isBigger


def incrementClock():
	theVal = int(causal_payload[IPPORT])
	++theVal
	causal_payload[IPPORT] == str(theVal)

def maxClocks(thisClock,incClock):
	for key in incClock:
		if key in thisClock:
			if incClock[key] > thisClock[key]:
				thisClock[key] = incClock[key]
		else:
			thisClock[key] = incClock[key]
	return thisClock

def getLiveIP(ipList):
	for ip in ipList:
		if ping(ip):
			return ip
	return "ERROR"


def ping(ip):
	resp = os.system("ping -c 1 " +  ip[:-5])
	if resp == 0:
		return True
	else:
		return False

def ryan_hash(some_string):
    hashvalue = 0
    i = 1
    # convert string to char array
    c_array = list(some_string)
    for char in c_array:
        hashvalue += pow(ord(char)*666 , ord(char)*i*666 % 7 )
        i+=1
    return hashvalue


if __name__ == "__main__":
	port = int(os.environ.get('PORT', 8080))
	app.run(host='0.0.0.0' , port=port, threaded=True)
