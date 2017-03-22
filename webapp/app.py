from flask import Flask, request, jsonify, json
from flask_api import status
app = Flask(__name__)
import os
import requests as req
theDict = dict()
IPPORT = os.getenv('IPPORT')

if 'VIEW' in os.environ:
	ip_view = os.getenv('VIEW').split(',')
else:
	ip_view = list()


@app.route("/kvs/view_update", methods=["PUT"])
def view_update():
	node = request.form.get("ip_port")
	type = request.args.get("type")
	
	hashView = list()
	for ip in ip_view:
		hashView.append(ip)

	if type == "add":
		if node not in hashView:
			hashView.append(node)
	elif type == "remove":
		if node in hashView:
			hashView.remove(node)

	
	for server in ip_view: # update every server in this servers IP view with new node
		if server != IPPORT: # send a new request to server
			req.put("http://" + server + "/kvs/removeView", timeout=1)
			for server2 in hashView:
				response = req.put("http://" + server + "/kvs/add/" + server2, timeout=1)

	req.put("http://" + node + "/kvs/removeView", timeout=1)
	for server in hashView:
		response = req.put("http://" + node + "/kvs/add/" + server, timeout=1)

	someDict = dict()
	theLen = int(len(hashView))
	for server in ip_view:
		if server != IPPORT:
			someRes = req.get("http://" + server + "/kvs/getDict", timeout=1)
			someDict = json.loads(someRes.content)
			someRes = req.get("http://" + server + "/kvs/delDict", timeout=1)
			for key in someDict:
				nodeLocate = ryan_hash(key) % theLen
				theValue = someDict[key]
				if IPPORT != hashView[nodeLocate]:
					print("server: " + hashView[nodeLocate])
					print("key: " + key)
					url_str = 'http://' + hashView[nodeLocate] + '/kvs/' + key
					someRes = req.put(url_str,data={'val':theValue}, timeout=6)
				else:
					theDict[key] = theValue

	delList = list()
	print("yee")
	for key in theDict:
		nodeLocate = ryan_hash(key) % theLen
		print("key: " + key)
		if IPPORT != hashView[nodeLocate]:
			theValue = theDict[key]
			url_str = "http://" + hashView[nodeLocate] + "/kvs/" + key
			print("IPPort local dicT: " + hashView[nodeLocate] + " value: " + theValue)
			req.put(url_str,data={'val':theValue}, timeout=5)
			print("we here1")
			delList.append(key)
		else:
			print("yoyooo")

	print("we here2")
	for key in delList:
		del theDict[key]
		
	print("we here3")	

	if type == "remove":
		someRes = req.get("http://" + node + "/kvs/delDict", timeout=5)
		if node in ip_view:
			someRes = req.put("http://" + node + "/kvs/removeView", timeout=5)
			ip_view.remove(node)
	elif type == "add":
		if node not in ip_view:
			ip_view.append(node)


	retDict = {'msg':'success'}
	return jsonify(retDict), status.HTTP_200_OK


@app.route('/kvs/<key>', methods = ['GET', 'PUT','DELETE']) 
def index(key):
	theLen = int(len(ip_view))
	#print('hash: ' + str(ryan_hash(key)))
	#print('keyX' + str(key) + 'X')
	nodeLocate = ryan_hash(key) % theLen
	#print ("IPPORT: " + IPPORT + " ip_view: " + ip_view[nodeLocate])
	#print("nodelocate: " + str(nodeLocate))
	#print("len: " + str(theLen))
	print("nodeLocater: " + ip_view[nodeLocate] + " len: " + str(theLen))
	if ip_view[nodeLocate] == IPPORT:
		if request.method == 'PUT':
			#print('put')
			if key in theDict:
				#print('indict')
				content = 'replaced: 1, // 0 if key did not exist msg: success\n'
				helpRet = 1
			else:
				content = 'replaced: 0, // 1 if an existing keys val was replaced\n msg success\n'
				#print('not in dict')
				helpRet = 0
			theValue = request.form.get('val')
			#print('val: ' + str(theValue))
			theDict[key] = theValue
			if helpRet == 1:
				retDict = {'replaced':'1, // 0 if key did not exist','msg':'success', 'owner':ip_view[nodeLocate]}
				return jsonify(retDict), status.HTTP_200_OK
			else:
				retDict = {'replaced':'0, // 1 if an existing keys val was replaced','msg':'success', 'owner':ip_view[nodeLocate]}
				return jsonify(retDict), status.HTTP_201_CREATED
		elif request.method == 'DELETE':
			if key in theDict:
				del theDict[key]
				content = 'msg : success\n'
				retDict = {'msg':'success','owner':ip_view[nodeLocate]}
				return jsonify(retDict), status.HTTP_200_OK
			else:
				content = 'msg : error\nerror : key does not exist\n'
				retDict = {'msg':'error','error':'key does not exist'}
				return jsonify(retDict), status.HTTP_404_NOT_FOUND	
		elif request.method == 'GET':
			if key in theDict:
				theRet = theDict[key]
				retDict = {'msg':'success', 'value':theRet, 'owner': ip_view[nodeLocate]}
				return jsonify(retDict),status.HTTP_200_OK
			else:
				retDict = {'msg':'error', 'error':'key does not exist'}
				return jsonify(retDict),status.HTTP_404_NOT_FOUND
		else:
			return jsonify('Error: Please call a GET, PUT or DELETE method.\n')
	else:
		url_str = 'http://' + ip_view[nodeLocate] + '/kvs/' + key
		#print('urlstr: ' + url_str)
		if request.method == 'PUT':
			theValue = request.form.get('val')
			req.data = theValue
			#print('value: ' + theValue)
			resp = req.put(url_str,data=dict(val=theValue))
			dRet = json.loads(resp.content)
			return jsonify(dRet), resp.status_code
		elif request.method == 'GET':
			resp = req.get(url_str)
			dRet = json.loads(resp.content)
			return jsonify(dRet), resp.status_code
		elif request.method == 'DELETE':
			resp = req.delete(url_str)
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
	return jsonify("del complete")


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
	app.run(host='0.0.0.0' , port=port)
