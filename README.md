# Approximate Vector Search

Insight Data Engineering Project, Boston, April - June 2018

***

<img src="demo/screencast.gif" height=320 width="auto"/>

## Overview

I implemented Elasticsearch-Aknn, an Elasticsearch plugin which adds 
functionality for approximate nearest neighbors search over dense,
floating-point vectors. 

To demonstrate the plugin, I used it to implement an image similarity search 
engine for a corpus of about 7 million Twitter images. The images were 
transformed into 1000-dimensional feature vectors using a convolutional neural 
network, and Elasticsearch-Aknn is used to store these vectors and search for 
nearest neighbors on an Elasticsearch cluster.

## Demo

[Screencast demo on Youtube.](https://www.youtube.com/watch?v=HqvbbwmY-0c)

<!-- [![Youtube Demo Thumbnail](https://img.youtube.com/vi/HqvbbwmY-0c/hqdefault.jpg)](https://www.youtube.com/watch?v=HqvbbwmY-0c) -->

[Web-application.](http://ec2-18-204-52-148.compute-1.amazonaws.com:9999/twitter_images/twitter_image/demo) It will likely go away once the Insight program ends.

[Presentation on Google Slides.](https://docs.google.com/presentation/d/1kkzwM-m5KvpfQFhqCepnR45_Pqz4poxPLtpbs3C8cXA/edit?usp=sharing)

## Elasticsearch-Aknn

More details coming soon.

## Image Processing Pipeline

More details coming soon.