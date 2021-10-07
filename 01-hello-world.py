# https://charlesleifer.com/blog/redis-streams-with-python/
# fazer a conta no Redis Labs

import os
from walrus import Database  # A subclass of the redis-py Redis client.
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(".env")

url = os.environ.get('REDIS_HOST')
port = os.environ.get('REDIS_PORT')
pw = os.environ.get('REDIS_PW')

db = Database(host=url, port=port, db=0, password=pw)
stream = db.Stream('stream-hello')

# msgid1 = stream.add({'message': 'mensagem 1'})
# msgid2 = stream.add({'message': 'mensagem 2'})
# msgid3 = stream.add({'message': 'mensagem 3'})
# print(msgid1)
# print(msgid2)
# print(msgid3)

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

# msgs = stream[msgid::2]
# print("\n2 mensagens a partir da mensagem 2:")
# print(msgs)

# # todos as mensagens
# msgs = list(stream)
# print("\nTodas as mensagens:")
# print(msgs)
