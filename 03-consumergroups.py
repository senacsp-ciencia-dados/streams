# https://charlesleifer.com/blog/redis-streams-with-python/
# fazer a conta no Redis Labs
# https://walrus.readthedocs.io/en/latest/

import os
from walrus import Database  # A subclass of the redis-py Redis client.
from dotenv import load_dotenv

load_dotenv(".env")

url = os.environ.get('REDIS_HOST')
port = os.environ.get('REDIS_PORT')
pw = os.environ.get('REDIS_PW')

db = Database(host=url, port=port, db=0, password=pw)

stream_keys = ['stream-a', 'stream-b', 'stream-c']
for stream in stream_keys:
    db.xadd(stream, {'data': ''})

# Create a consumer-group for streams a, b, and c. We will mark all
# messages as having been processed, so only messages added after the
# creation of the consumer-group will be read.
cg = db.consumer_group('cg-abc', stream_keys)
cg.create()  # Create the consumer group.
cg.set_id('$')