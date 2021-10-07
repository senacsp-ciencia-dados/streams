# https://charlesleifer.com/blog/redis-streams-with-python/
# fazer a conta no Redis Labs

import os
from walrus import Database  # A subclass of the redis-py Redis client.
from dotenv import load_dotenv

load_dotenv(".env")

url = os.environ.get('REDIS_HOST')
port = os.environ.get('REDIS_PORT')
pw = os.environ.get('REDIS_PW')

db = Database(host=url, port=port, db=0, password=pw)
stream = db.Stream('stream-hello')

msgid = stream.add({'message': 'mensagem 1'})
msgid2 = stream.add({'message': 'mensagem 2'})
msgid3 = stream.add({'message': 'mensagem 3'})
print(msgid)

msgs = stream[msgid2:]
print("\nA partir da mensagem 2:")
print(msgs)

msgs = stream[msgid::2]
print("\n2 mensagens a partir da mensagem 2:")
print(msgs)

# todos as mensagens
msgs = list(stream)
print("\nTodas as mensagens:")
print(msgs)
