import grpc
from concurrent import futures
import time
import datetime

# import the generated classes
import grpc_remote_procedure_pb2
import grpc_remote_procedure_pb2_grpc

# import the original .py
import grpc_remote_procedure

class addServicer(grpc_remote_procedure_pb2_grpc.addServicer):
    def add(self, request, context):
        response = grpc_remote_procedure_pb2.addMsgReturn()
        response.ans = grpc_remote_procedure.add(request.a, request.b)
        return response
    
    
class imageServicer(grpc_remote_procedure_pb2_grpc.imageServicer):    
    def image(self, request, context):
        response = grpc_remote_procedure_pb2.imageMsgReturn()
        reply = grpc_remote_procedure.image(request.img)
        response.width = reply[0]
        response.height = reply[1]
        return response
    
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

grpc_remote_procedure_pb2_grpc.add_addServicer_to_server(addServicer(), server)

grpc_remote_procedure_pb2_grpc.add_imageServicer_to_server(imageServicer(), server)


print('Starting server. Listening on port 5000.')
server.add_insecure_port('[::]:5000')
server.start()


#since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)
