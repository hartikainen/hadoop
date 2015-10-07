#!/bin/bash

INPUT_FILE=/input/author_book_tuple.txt
OUTPUT_PATH=/output/query-author
AUTHOR="J. K. Rowling"

echo "running QueryAuthor"
echo "Hadoop should be up and running"
echo "=============================================="

echo hadoop com.sun.tools.javac.Main QueryAuthor.java
hadoop com.sun.tools.javac.Main QueryAuthor.java
echo jar cf qa.jar QueryAuthor*.class
jar cf qa.jar QueryAuthor*.class
echo hadoop fs -rm -f -r $OUTPUT_PATH
hadoop fs -rm -f -r $OUTPUT_PATH
echo hadoop jar qa.jar QueryAuthor $INPUT_FILE $OUTPUT_PATH $AUTHOR
hadoop jar qa.jar QueryAuthor $INPUT_FILE $OUTPUT_PATH $AUTHOR

echo
echo "=============================================="
echo "wrote output to: " $OUTPUT_PATH
