# https://charlesleifer.com/blog/redis-streams-with-python/
# fazer a conta no Redis Labs
# https://walrus.readthedocs.io/en/latest/

import os
from walrus import Database  # A subclass of the redis-py Redis client.
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(".env")

url = os.environ.get('REDIS_HOST')
port = os.environ.get('REDIS_PORT')
pw = os.environ.get('REDIS_PW')

db = Database(host=url, port=port, db=0, password=pw)

# Add 1000 items to "stream-2".
stream2 = db.Stream('stream-tempo')
for i in range(1000):
    stream2.add({'data': 'mensagem-%s' % i})

# Trim stream-2 to (approximately) 10 most-recent messages.

data_e_hora_em_texto = "07/10/2021 19:30:00"
data_e_hora = datetime.strptime(data_e_hora_em_texto, "%d/%m/%Y %H:%M:%S")
start = str(int(datetime.timestamp(data_e_hora))) + "000-0"
print("timestamp =", start)

data_e_hora_em_texto = "07/10/2021 19:31:00"
data_e_hora = datetime.strptime(data_e_hora_em_texto, "%d/%m/%Y %H:%M:%S")
end = str(int(datetime.timestamp(data_e_hora))) + "000-0"
print("timestamp =", end)

msgs = stream[start:end]
print("\nA partir da mensagem " + start + ", são " + str(len(msgs)))
print(msgs)