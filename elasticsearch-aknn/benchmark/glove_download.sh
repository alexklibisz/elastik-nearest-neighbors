#!/bin/sh
set -e
wget http://nlp.stanford.edu/data/glove.twitter.27B.zip
unzip glove.twitter.27B.zip
rm glove.twitter.27B.50d.txt glove.twitter.27B.100d.txt glove.twitter.27B.200d.txt
echo "Done"
