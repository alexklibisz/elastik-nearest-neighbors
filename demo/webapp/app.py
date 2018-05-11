from flask import Flask, request, render_template
from itertools import cycle
from pprint import pprint
import os
import pdb
import requests

# Get elasticsearch hosts from environment variable.
ESHOSTS = cycle(["http://localhost:9200"])
if "ESHOSTS" in os.environ:
    ESHOSTS = cycle(os.environ["ESHOSTS"].split(","))

SAFE_RANDOM_IDS = cycle([
    "991422991845163009",
    "988567576799272960",
    "992424335796125697",
    "990177895921344514",
    "988992585619259392",
    "990790859904794626",
    "989815445132750849",
    "991408722823000065",
    "990656344377167872",
    "988552330512687104"
])

app = Flask(__name__)


@app.route("/")
def index():
    return "Hello, Internet"


@app.route("/<es_index>/<es_type>/<es_id>")
def images(es_index, es_type, es_id):

    if es_id.lower() == "random":
        es_id = next(SAFE_RANDOM_IDS)

    # Fetch 10 random images for bottom carousel.
    body = {
        "_source": ["s3_url"],
        "size": 20,
        "query": {
            "function_score": {
                "query": {"match_all": {}},
                "boost": 5,
                "random_score": {},
                "boost_mode": "multiply"
            }
        }
    }
    req_url = "%s/%s/%s/_search" % (next(ESHOSTS), es_index, es_type)
    req = requests.get(req_url, json=body)
    random_imgs = req.json()["hits"]["hits"]

    # Get number of docs in corpus.
    req_url = "%s/%s/%s/_count" % (next(ESHOSTS), es_index, es_type)
    req = requests.get(req_url)
    count = req.json()["count"]

    # Get the nearest neighbors for the query image, which includes the image.
    image_id = request.args.get("image_id")
    req_url = "%s/%s/%s/%s/_aknn_search?k1=100&k2=10" % (
        next(ESHOSTS), es_index, es_type, es_id)
    req = requests.get(req_url)
    hits = req.json()["hits"]["hits"]
    took_ms = req.json()["took"]
    query_img, neighbor_imgs = hits[0], hits[1:]

    # Render template.
    return render_template(
        "index.html",
        es_index=es_index,
        es_type=es_type,
        took_ms=took_ms,
        count=count,
        query_img=query_img,
        neighbor_imgs=neighbor_imgs,
        random_imgs=random_imgs)
