#!/bin/bash

INPUT_FILE=/input/vanrikki-stool.txt
OUTPUT_PATH=/output/top-count

echo "running WordCount example on file"
echo "Hadoop should be up and running"
echo "=============================================="

hadoop com.sun.tools.javac.Main TopCount.java
jar cf tc.jar TopCount*.class

hadoop jar tc.jar TopCount $INPUT_FILE $OUTPUT_PATH

echo
echo "=============================================="
echo "wrote output to: " $OUTPUT_PATH
