#!/bin/bash

INPUT_FILE=/input/author_book_tuple.txt
OUTPUT_PATH=/output/combine-books

echo "running CombineBooks"
echo "Hadoop should be up and running"
echo "=============================================="

hadoop com.sun.tools.javac.Main CombineBooks.java
jar cf tc.jar CombineBooks*.class

hadoop jar tc.jar CombineBooks $INPUT_FILE $OUTPUT_PATH

echo
echo "=============================================="
echo "wrote output to: " $OUTPUT_PATH
