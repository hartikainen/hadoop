#!/bin/bash

INPUT_FILE=/vanrikki-stool.txt
OUTPUT_PATH=/output

echo "running WordCount example on file"
echo "Hadoop should be up and running"
echo "=============================================="

hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class

hadoop jar wc.jar WordCount $INPUT_FILE $OUTPUT_PATH

echo
echo "=============================================="
echo "wrote output to: " $OUTPUT_PATH
