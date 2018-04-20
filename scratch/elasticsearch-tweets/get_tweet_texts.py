"""Read twitter statuses from disk and print out only their text"""
from glob import glob
import json

if __name__ == "__main__":

    statuses_dir = '/home/alex/Desktop/statuses'

    for path in glob('%s/*' % statuses_dir):

        # Attempt to read status as JSON file. Doesn't always work.
        try:
            with open(path) as fp:
                status = json.load(fp)
        except json.decoder.JSONDecodeError as ex:
            continue

        if 'full_text' in status:
            text = status['full_text']
        else:
            text = status['text']

        print(text.replace('\n', ''))
