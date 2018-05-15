Flask web application for Twitter Image Similarity search.

The web app consists of a single endpoint which executes a similarity search
against an external Elasticsearch node and serves a web page containing the
results for that search, as well as several random images from the index so
the user can continue browsing.

## Usage

```
# Install Flask
pip3 install flask

# Define one or more comma-separated Elasticsearch host urls
# http://localhost:9200 is the default.
export ESHOSTS="http://localhost:9200"

# Define and run the app (in development mode).
export FLASK_APP=app.py
python3 -m flask run -p 9999 -h 0.0.0.0
```
