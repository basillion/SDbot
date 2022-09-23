import requests
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import engine as sql
import pymysql
import telebot
from loguru import logger
import os

myToken = os.environ["SDTOKEN"]
connstr = os.environ["CONNSTR"]
teletoken = os.environ["TELETOKEN"]
myHead = {'Authtoken': myToken}
ids = []
subjects = []
users = []
specs = []
statuses = []

logger.debug("Sending request")
req = requests.get(
    'https://sd.servolux.by/api/v3/requests?input_data=%7B%0A%22list_info%22%3A%20%7B%0A%22search_criteria%22%3A%20%7B%0A%22condition%22%3A%20%22is%22%2C%0A%22field%22%3A%20%22technician.name%22%2C%0A%22values%22%3A%20%5B%20%22%D0%95%D0%B2%D0%B3%D0%B5%D0%BD%D0%B8%D0%B9%20%D0%90%D0%BB%D0%B5%D0%BA%D1%81%D0%B0%D0%BD%D0%B4%D1%80%D0%BE%D0%B2%D0%B8%D1%87%20%D0%A2%D0%B8%D1%85%D0%BE%D0%BD%D0%BE%D0%B2%D0%B8%D1%87%22%2C%0A%22%D0%9D%D0%B8%D0%BA%D0%B8%D1%82%D0%B0%20%D0%9D%D0%B8%D0%BA%D0%BE%D0%BB%D0%B0%D0%B5%D0%B2%D0%B8%D1%87%20%D0%9F%D0%B5%D1%80%D0%B5%D0%BF%D0%B5%D1%87%D0%B8%D0%BD%22%0A%5D%0A%7D%2C%0A%22row_count%22%3A%20100%0A%7D%0A%7D',
    headers=myHead, verify=False)  # get request to API
data = req.content.decode()  # Decoding the answer for correct showing of cyrillic symbols
req.close()
#print(data)
jsonObj = json.loads(data)
rqsts = jsonObj['requests']
#print(rqsts)


logger.debug("Working with requests")
for rqst in rqsts:
    ids.append(rqst['id'])
    subjects.append(rqst['subject'])
    users.append(rqst['requester']['name'])
    if rqst['technician'] is None:
        specs.append('Неназначено')
    else:
        specs.append(rqst['technician']['name'])
    statuses.append((rqst['status']['name']))

logger.debug("Connecting to database")
# Создадим соединение с БД
eng = sql.create_engine(f"mysql+pymysql://{connstr}")
conn = eng.connect()
logger.debug("Truncating tables")
truncate_query = sqlalchemy.text("TRUNCATE TABLE requests")
conn.execution_options(autocommit=True).execute(truncate_query)

logger.debug("Loading new requests to pandas")
# Создаем пандосовский датафрейм, который затем сохраняем в БД в таблицу
df = pd.DataFrame({'id': ids, 'subject': subjects, '_user': users, '_spec': specs, '_status': statuses})
logger.debug("Loading a dataframe to the database")
df.to_sql('requests', conn, if_exists='append', index=False)
conn.close()
logger.debug("Function SDjson finished!")

logger.debug("Reading a telegram token")

def send_message(message, CHAT_ID):
    bot = telebot.TeleBot(token=teletoken)
    bot.send_message(chat_id=CHAT_ID, text=message)

logger.debug("Connecting to tha database")
eng = sql.create_engine(f"mysql+pymysql://{connstr}")
conn = eng.connect()
query = "SELECT id, subject, _user, _spec, spec.id_t FROM requests JOIN spec ON " \
        "requests._spec = spec.spec_name WHERE _status = \'Открыта\' AND spec.subscribe = 1"
reqs = conn.execute(query).fetchall()
logger.debug("SELECT query executed!")
conn.close()

for req in reqs:
    message = "Здравствуйте, {_spec}!\n У Вас есть открытая заявка №{id}" \
              " от пользователя {_user}" \
              " на тему: \"{subject}\"".format(_spec=req[3], id=str(req[0]), _user=req[2], subject=req[1])
    send_message(message, req[4])
    logger.debug("A message was sended")

