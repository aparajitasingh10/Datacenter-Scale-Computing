import sys

import grpc
from time import perf_counter


# import the generated classes
import grpc_remote_procedure_pb2
import grpc_remote_procedure_pb2_grpc


port = 5000


def func():
    
    if arg == "add":
        
        x = int(sys.argv[4])
        y = int(sys.argv[5])
        
        Msg = grpc_remote_procedure_pb2.addMsg(a=x, b=y)
        
        stub = grpc_remote_procedure_pb2_grpc.addStub(channel)
        
        t1_start = perf_counter()  
        for counter in range(iterations):
            response = stub.add(Msg)
            print("Add result: ", response.ans)
        
        t1_stop = perf_counter()  
        time_add = t1_stop - t1_start
        avg_time_add = time_add / iterations
        print("Average time taken for {} iterations for {} endpoint: {}".format(iterations, arg.upper(), avg_time_add))
        
        
    if arg == "image":
        
        img = open('Flatirons_Winter_Sunrise_edit_2.jpg', 'rb').read()
        Msg = grpc_remote_procedure_pb2.imageMsg(img=img)
        
        stub = grpc_remote_procedure_pb2_grpc.imageStub(channel)
        
        t1_start = perf_counter()  
        for counter in range(iterations):
            response = stub.image(Msg)
            print("Image dimensions are: width - {}, height- {} ".format(response.width, response.height))
        
        t1_stop = perf_counter()  
        time_image = t1_stop - t1_start
        avg_time_image = time_image / iterations
        print("Average time taken for {} iterations for {} endpoint: {}".format(iterations, arg.upper(), avg_time_image))
        
if __name__ == '__main__':
    
    #ip to reach
    ip = sys.argv[1]
    #endpoint to be tested
    arg = sys.argv[2]
    #number of iterations to test
    iterations = int(sys.argv[3])

    addr = ip+":"+str(port)
    
    channel = grpc.insecure_channel(addr)
    
    func()