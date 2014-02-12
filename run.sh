#!/bin/sh
if [ -f result ] 
then
	rm result
fi
javac -classpath `hadoop classpath` $1
if [ $? -ne 0 ]
then 
	exit 
fi
jar -cvf tmp.jar *.class
hadoop fs -rmr /home/test/output/
hadoop fs -rm /home/test/input/input.txt
hadoop fs -put input.txt /home/test/input

if hadoop jar tmp.jar `echo $1 | cut -d. -f1`  /home/test/input/ /home/test/output/; then
	hadoop fs -ls /home/test/output
	hadoop fs -get /home/test/output/part-r-00000 result
#	cat result
fi
rm tmp.jar
rm *.class

