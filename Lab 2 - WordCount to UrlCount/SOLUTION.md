Lab 2 Datacenter Scale Computing

I used the Hadoop streaming interface
- Created a program for mapper and its name is URLMapper.py
- Created a program for reducer and its name is URLReducer.py
- Made necessary changes in Makefile

Software needed to run:
- Coding csel
- Dataproc on GCP

Resources used:
- https://www.geeksforgeeks.org/python-check-url-string/
- qwiklab tutorial

People I discussed with:
- Amit Baran Roy
- Nivetha Kesavan

To run on Dataproc:
- Used qwiklab credits to run Dataproc
- Created cluster with 2 nodes and then with 4 nodes
- Created filesystem using make filesystem
- Ran make prepare
- Ran command "sudo apt-get install time" to install the time library
- Ran make stream to run the code

QUESTION 1

The Java WordCount implementation used a Combiner to improve efficiency, but that may cause problems for this application and produce a different output. Explain why this would be the case (even if you didn't implement the Java version).

ANSWER 1

As mentioned in class, combiner uses the same code as reducer. However, in our code, we are giving as output only those URLs that appear for more than 5 times in total. If we use a combiner here, it will ignore the URLs that have lesser than 5 count after the individual Mapper phase. This will therefore not give the right / required output.

QUESTION 2

You should also include a comparison of the 2-node and 4-node execution time. Discuss the execution times and any suprising outcomes.

ANSWER 2

Time taken with 2 nodes: 7.68
Time taken with 4 nodes: 7.89 
The time taken by 4 nodes is greater, which is a surprising outcome. One would assume that 4 nodes would take lesser time. However, in this case, 4 nodes take more time to execute. This is because the 4 node cluster performs more map and reduce tasks than the 2 node cluster. This is the case in this problem where the data was less. When dealing with more data, using more nodes will be beneficial.

