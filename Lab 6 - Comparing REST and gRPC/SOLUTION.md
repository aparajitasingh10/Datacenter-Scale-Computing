COLLABORATOR: Amit Baran Roy
RESOURCES: Links provided

|  Method 	    | Local  	| Same-Zone  	|  SameReg/Diff Zone 	| Europe    |
|-------------- |---------- |-------------- |-----------------------|-----------|
|   REST add	|   2.54ms	|   2.90ms	    |  	  3.1ms             |  280.60ms
|   gRPC add	|   0.67ms  |   0.65ms	    |     0.59ms	        |  142.72ms
|   REST img	|   4.88ms	|   23.06ms 	|     23.06ms           |  1.15s
|   gRPC img	|   4.94ms  |   7.33ms  	|     7.12ms            |  172.45ms
|   PING        |   0.036ms |   0.264ms     |     0.287ms           |  138ms

You should measure the basic latency  using the `ping` command - this can be construed to be the latency without any RPC or python overhead.

You should examine your results and provide a short paragraph with your observations of the performance difference between REST and gRPC. You should explicitly comment on the role that network latency plays -- it's useful to know that REST makes a new TCP connection for each query while gRPC makes a single TCP connection that is used for all the queries.

ANSWER:

1. REST makes a new TCP connection for each query while gRPC makes a single TCP connection that is used for all the queries. Because of TCP overhead, REST is slower than gRPC.
gRPC uses protocol buffers and HTTP/2. Using HTTP/2, it can multiplex multiple RPCs on the same TCP connection.

2. The latency is the time it takes for data or a request to go from the source to the destination. As the latency increases across the different regions, the time taken also increases. TCP expects round trip acknowledgement, therefore higher latency effects its performance.
