"""
Minimal Flask web app to demonstrate Elasticsearch-Aknn functionality
on corpus of Twitter image features.
"""

from flask import Flask, request, render_template, redirect
from itertools import cycle
from pprint import pprint
import os
import random
import requests

# Get elasticsearch hosts from environment variable.
ESHOSTS = cycle(["http://localhost:9200"])
if "ESHOSTS" in os.environ:
    ESHOSTS = cycle(os.environ["ESHOSTS"].split(","))

# Define a set of images to cycle through for the /demo endpoint.
DEMO_IDS = [
    "988221425063530502",  # Mountain scenery
    "990013386929917953",  # Car
    "991780208138055681",  # Screenshot
    "989646964148133889",  # Male actors (DiCaprio)
    "988889393158115329",  # Male athlete (C. Ronaldo)
    "988487255877718017",  # Signs
    "991004064748978177",  # Female selfie
    "988237522810503168",  # Cartoon character
    "989808637773135873",  # North/south Korean politicians
    "989144784341229568",  # Leo Messi
    "989655776363921409",  # Dog
    "991484266415443968",  # Some kids playing with a racoon
    "989836022384156672",  # Mountain scenery
    "990578938505146368",  # Race cars
    "988526279665205248",  # Store fronts
    "989477367486672896",
    "988531509954011139",
    "990159780726665216",
    "990678809081823232",
    "992379356071825410",
    "988788327217119233",
    "989065251919458304",
    "989617448843403264",
    "990863324890869760",
    "989664366319484928",
    "989951906809344001",
    "988674636417249281",
    "988426706888216576",
    "991450758120902656",
    "990226717607415808",
    "988902080923529217",
    "990372146735087616",
    "989678396274814976",
    "988867339516022784",
    "990713839892119552",
    "992056122050662400",
    "989161016280875008",
    "990594050557231104",
    "992186954941980673",
    "988825283204558848",
    "989350699472490497",
    "990430615324450816"
]

app = Flask(__name__)


@app.route("/slides")
def slides():
    return redirect("https://docs.google.com/presentation/d/1AyIyBqzCqKhytZWcQfSEhtBRN-iHUldBQn14MGGKpr8/present", 
                    code=302)

@app.route("/")
@app.route("/demo")
def demo():
    return redirect("/twitter_images/twitter_image/demo", code=302)

@app.route("/<es_index>/<es_type>/<es_id>")
def images(es_index, es_type, es_id):

    

    # Parse elasticsearch ID. If "demo", pick a random demo image ID.
    if es_id.lower() == "demo":
        es_id = random.choice(DEMO_IDS)

    elif es_id.lower() == "random":
        body = {
            "_source": ["s3_url"],
            "size": 1,
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
        es_id = req.json()["hits"]["hits"][0]["_id"]

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
        neighbor_imgs=neighbor_imgs)
