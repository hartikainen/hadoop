#!/bin/bash

INPUT_FILE=/input/author_book_tuple.txt
OUTPUT_PATH=/output/combine-books

echo "running CombineBooks"
echo "Hadoop should be up and running"
echo "=============================================="

# $ javac -cp "`yarn classpath`:/home/hadoop/hadoop-2.7.1/lib/java-json.jar" CombineBooks.java

hadoop com.sun.tools.javac.Main CombineBooks.java
jar cf cb.jar CombineBooks*.class
hadoop fs -rm -f -r $OUTPUT_PATH
hadoop jar cb.jar CombineBooks $INPUT_FILE $OUTPUT_PATH

echo
echo "=============================================="
echo "wrote output to: " $OUTPUT_PATH
