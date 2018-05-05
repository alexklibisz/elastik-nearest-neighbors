"""
Construct an index of S3 bucket and key names for downstream processing.
"""
from argparse import ArgumentParser
import os

if __name__ == "__main__":

    ap = ArgumentParser(description="See script header")
    ap.add_argument("images_dir", help="Path to directory containing images")
    ap.add_argument("-b", "--bucket_name", help="Name of the S3 bucket", default="klibisz-twitter-stream")
    args = vars(ap.parse_args())
    
    for fobj in os.scandir(args["images_dir"]):
        print(args["bucket_name"], fobj.name)
        
