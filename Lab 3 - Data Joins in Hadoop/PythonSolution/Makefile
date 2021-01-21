##
## Makefile

run: input/cite75_99.txt input/apat63_99.txt
	-rm -rf output1
	./RUN-MAP-REDUCE \
		-files mapperone.py,reducerone.py \
		-mapper mapperone.py \
		-reducer reducerone.py \
		-input input -output output1

	-rm -rf output2
	-cp input/apat63_99.txt output1
	./RUN-MAP-REDUCE \
		-files mappertwo.py,reducertwo.py \
		-mapper mappertwo.py \
		-reducer reducertwo.py \
		-input output1 -output output2

	-rm -rf output3
	-cp input/apat63_99.txt output2
	./RUN-MAP-REDUCE \
		-files mapperthree.py,reducerthree.py \
		-mapper mapperthree.py \
		-reducer reducerthree.py \
		-input output2 -output output3

input:
	mkdir ./input

input/cite75_99.txt: input ../acite75_99.zip
	unzip ../acite75_99.zip
	touch cite75_99.txt # update modification time
	mv cite75_99.txt input

input/apat63_99.txt: input ../apat63_99.zip
	unzip ../apat63_99.zip
	touch apat63_99.txt # update modification time
	mv apat63_99.txt input

prepare:
	echo "Some of the following commands may fail because your"
	echo "files are already setup"
	echo "====="
	-hdfs dfs -mkdir /user
	-hdfs dfs -mkdir /user/$(USER)
	-hdfs dfs -rm -r -f /user/$(USER)/input
	-hdfs dfs -rm -r -f /user/$(USER)/output	


