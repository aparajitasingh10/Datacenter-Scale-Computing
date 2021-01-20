#!/usr/bin/env python3
from __future__ import print_function
import requests
import json
from time import perf_counter
import sys

addr = 'http://localhost:5000'

port = 5000
#ip to reach
ip = sys.argv[1]
#endpoint to be tested
arg = sys.argv[2]
#number of iterations to test
iterations = int(sys.argv[3])

addr = "http://"+ip+":"+str(port)

# prepare headers for http request
headers = {'content-type': 'image/png'}
headers_add = {'content-type': 'application/json'}

img = open('Flatirons_Winter_Sunrise_edit_2.jpg', 'rb').read()

# send http request with image and receive response
if arg == 'image':
    
    #endpoint
    image_url = addr + '/api/image'
    
    print("Endpoint for {}: {}".format(arg, image_url))
    
    t1_start = perf_counter()  
    for counter in range(iterations):
        
        response = requests.post(image_url, data=img, headers=headers)
        print("Image dimensions are: width - {}, height- {} ".format(json.loads(response.text)['width'], json.loads(response.text)['height']))

    t1_stop = perf_counter()  
    time_image = t1_stop - t1_start
    avg_time_image = time_image / iterations
    print("Average time taken for {} iterations for {} endpoint: {}".format(iterations, arg, avg_time_image))
    
# send http request for addition and receive response
if arg == 'add':
    
    x = sys.argv[4]
    y = sys.argv[5]

    #endpoint
    addition_url = addr + '/api/add/' + x + '/' + y
    
    print("Endpoint for {}: {}".format(arg, addition_url))
    
    t1_start = perf_counter()  
    
    for counter in range(iterations):
        
        response = requests.get(addition_url, headers=headers_add)
        print("Add result: ", json.loads(response.text)['addition'])
    
    t1_stop = perf_counter()  
    time_add = t1_stop - t1_start
    avg_time_add = time_add / iterations
    print("Average time taken for {} iterations for {} endpoint: {}".format(iterations, arg, avg_time_add))