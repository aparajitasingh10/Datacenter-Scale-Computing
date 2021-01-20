#
# Worker server
#
import pickle, jsonpickle
import platform
from PIL import Image
import io
import os
import sys
import pika
import redis
import hashlib
import face_recognition
import codecs

def callback(ch, method, properties, body):
    try:
        print("{} - RabbitMQ message received - begin processing".format(hostname), file=sys.stderr)
        rabbitMQChannel.basic_publish(exchange='logs', routing_key=debug, body="RabbitMQ message received - begin processing")
        print (jsonpickle.loads(body))
        fileName = jsonpickle.loads(body)['fileName']
        imgHash = jsonpickle.loads(body)['imgHash']
        imgData = pickle.loads(codecs.decode(jsonpickle.loads(body)['imgData'].encode(), "base64"))

        redisNameToHash.set(fileName, imgHash)      # filename -> hash DB1
        print("Entry added to DB1 - redisNameToHash: <{}><{}>".format(fileName, imgHash), file=sys.stderr)
        rabbitMQChannel.basic_publish(exchange='logs', routing_key=info, body="Entry added to DB1 - redisNameToHash")
        rabbitMQChannel.basic_publish(exchange='logs', routing_key=debug, body="DB1: <{}><{}>".format(fileName, imgHash))
        redisHashToName.sadd(imgHash, fileName)     # hash -> [filename] DB2
        print("Entry added to DB2 - redisHashToName: <{}><{}>".format(imgHash, redisHashToName.smembers(imgHash)), file=sys.stderr)
        rabbitMQChannel.basic_publish(exchange='logs', routing_key=info, body="Entry added to DB2 - redisHashToName")
        rabbitMQChannel.basic_publish(exchange='logs', routing_key=debug, body="DB2: <{}><{}>".format(imgHash, redisHashToName.smembers(imgHash)))

        # Load the received image file
        img = face_recognition.load_image_file(imgData)
        # Get face encodings for any faces in the received image
        unknown_face_encodings = face_recognition.face_encodings(img)

        if len(unknown_face_encodings) > 0:
            for i in unknown_face_encodings:
                redisHashToFaceRec.sadd(imgHash, jsonpickle.dumps(i))     # hash -> [face encodings] DB3
                print("Entry added to DB3 - redisHashToFaceRec: <{}><{}>".format(imgHash, redisHashToFaceRec.smembers(imgHash)), file=sys.stderr)
                rabbitMQChannel.basic_publish(exchange='logs', routing_key=info, body="Entry added to DB3 - redisHashToFaceRec")
                rabbitMQChannel.basic_publish(exchange='logs', routing_key=debug, body="DB3: <{}><{}>".format(imgHash, redisHashToFaceRec.smembers(imgHash)))
                for key in redisHashToFaceRec.scan_iter():
                    known_face_encodings = [jsonpickle.loads(j) for j in redisHashToFaceRec.smembers(key)]
                    if any(face_recognition.compare_faces(known_face_encodings, i)):
                        redisHashToHashSet.sadd(imgHash, key)       # hash -> [hashes]  DB4
                        redisHashToHashSet.sadd(key, imgHash)       # hash -> [hashes]  DB4
                        print("Entries added to DB4 - redisHashToHashSet: <{}><{}>, <{}><{}>".format(imgHash, redisHashToHashSet.smembers(imgHash), key, redisHashToHashSet.smembers(key)), file=sys.stderr)
                        rabbitMQChannel.basic_publish(exchange='logs', routing_key=info, body="Entries added to DB4 - redisHashToHashSet")
                        rabbitMQChannel.basic_publish(exchange='logs', routing_key=debug, body="DB4: <{}><{}>, <{}><{}>".format(imgHash, redisHashToHashSet.smembers(imgHash), key, redisHashToHashSet.smembers(key)))

        print("{} - RabbitMQ message processed and acknowledged".format(hostname), file=sys.stderr)
        rabbitMQChannel.basic_publish(exchange='logs', routing_key=debug, body="RabbitMQ message processed and acknowledged")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    except Exception as e:
        #send negative ack
        print ("Error in callback: {}".format(e))
        rabbitMQChannel.basic_publish(exchange='logs', routing_key=debug, body="Error in callback: {}".format(e))
        channel.basic_nack(delivery_tag=method.delivery_tag)


        
hostname = str(platform.node())

##
## Configure test vs. production
##
redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"

print("Connecting to rabbitmq({}) and redis({})".format(rabbitMQHost,redisHost), file=sys.stderr)

##
## You provide this
##

try:
    #redist db connections
    redisNameToHash = redis.Redis(host=redisHost, db=1)    # Key -> Value
    redisHashToName = redis.StrictRedis(host=redisHost, db=2, charset="utf-8", decode_responses=True)    # Key -> Set
    redisHashToFaceRec = redis.StrictRedis(host=redisHost, db=3, charset="utf-8", decode_responses=True) # Key -> Set
    redisHashToHashSet = redis.StrictRedis(host=redisHost, db=4, charset="utf-8", decode_responses=True) # Key -> Set

    #rabbitMQ listener - listen to direct exchange messages sent by rest server
    rabbitMQ = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitMQHost))
    rabbitMQChannel = rabbitMQ.channel()

    rabbitMQChannel.exchange_declare(exchange='toWorker', exchange_type='direct')
    rabbitMQChannel.exchange_declare(exchange='logs', exchange_type='topic')
    result = rabbitMQChannel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = ['img']

    for key in binding_keys:
        rabbitMQChannel.queue_bind(
                exchange='toWorker', 
                queue=queue_name,
                routing_key=key)

    #set routing keys
    info = hostname + ".worker.info"
    debug = hostname + ".worker.debug"


    print ('start consume', file=sys.stderr)
    rabbitMQChannel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False)

    rabbitMQChannel.start_consuming()
    
except Exception as e:
    #restart required
    print("Exception occurred, need restart...\nDetail:\n%s" % e)
    try:
        connection.close()
    except:
        pass