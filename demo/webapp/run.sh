#!/bin/sh
export FLASK_APP=app.py
export ESHOSTS=http://ip-172-31-93-93.ec2.internal:9200,http://ip-172-31-88-105.ec2.internal:9200,http://ip-172-31-84-21.ec2.internal:9200,http://ip-172-31-87-186.ec2.internal:9200
python3 -m flask run -p 9999 -h 0.0.0.0
