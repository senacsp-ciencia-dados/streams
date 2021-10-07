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

# Add 1000 items to "stream-2".
stream2 = db.Stream('stream-tamanho')
for i in range(1000):
    stream2.add({'data': 'mensagem-%s' % i})

# # Trim stream-2 to (approximately) 10 most-recent messages.
# nremoved = stream2.trim(10)
# print(nremoved)
# # 909
# print(len(stream2))
# # 91

# # To trim to an exact number, specify `approximate=False`:
# stream2.trim(10, approximate=False)  # Returns 81.
# print(len(stream2))
# # 10


# stream2.read(timeout=2000, last_id='$')