# 5253 Datacenter Scale Computing
## Lab 3 - Hadoop Joins

Language: Python

Collaborators: None

Software needed to run: Coding csel

Resources used: Lecture videos and starter code

To run: When in lab3 directory, run 'make' command to execute the external Makefile which will download the input files.

When in PythonSolution directory, run 'make' command to execute the internal Makefile. The internal Makefile has been modified with appropriate changes.

The solution contains 6 files:

mapperone.py, mappertwo.py, mapperthree.py, reducerone.py, reducertwo.py, reducerthree.py

The makefile executes the three sets of mappers and reducers.

The output of mapper one is the input for reducer one. The output of reducer one is the input for mapper two. The output of mapper two is the input for reducer two. The output of reducer two is the input for mapper three. The output for mapper three is the input for reducer three.

#### Mapper One
Input: The two input files with <citing,cited> and patent info

The first mapper takes the above as input and gives an output as the following:

<cited, citing>

<cited, info>

#### Reducer One
Input: Output of mapper one

The first reducer combines the information from mapper one and gives an output as the following:

<cited, citing, cited_state>

(the cited_state is extracted from the info)

#### Mapper Two
Input: Output of reducer one and patent info file

The second mapper takes the above as input and gives an output of the following format:

<citing, cited, cited_state>

<citing, info>

#### Reducer Two
Input: Output of mapper two

The second reducer combines the information from mapper two and gives an output of the following format:

<citing, citing_state, cited, cited_state>

(the citing_state is extracted from the info)

#### Mapper Three
Input: Output of reducer two and patent info file

The third mapper takes the above as input and gives an output of the following format:

<citing, citing_state, cited_state, 1>

<citing, info>

#### Reducer Three
Input: Output of mapper three

The third reducer does the counting job using the output of mapper three. It then prints out the patent info as given in patent info file along with the count.
