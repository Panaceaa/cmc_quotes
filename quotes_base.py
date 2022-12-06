import time
import requests
from datetime import datetime
import json
import pandas as pd
import tqdm
import pymongo

headers = {
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36'}


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = pymongo.MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['crypto_data']


def history_quotes(token_id, start_time=1648166400, end_time=1653436800, q_dict=None):
    if q_dict is None:
        q_dict = {}
    sep = 7776000
    time.sleep(0.2)
    url = f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical?id={str(token_id)}&convertId=2781&timeStart={str(start_time)}&timeEnd={str(end_time)}'
    resp = session.get(url, headers=headers)
    content = json.loads(resp.text)['data']['quotes']
    dict_quotes = {datetime.strptime(x['timeClose'][:10], "%Y-%m-%d"): x['quote'] for x in content}
    q_dict = q_dict | dict_quotes
    if start_time < end_time:
        return dict(sorted(q_dict.items()))
    else:
        return history_quotes(token_id, start_time - sep, end_time - sep, q_dict)


today = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d')
compile_data = pd.DataFrame()
time_data_list = ['daily_active_holders', 'statistics', 'predictions', 'lastHalfYearStatistics', 'accuracyPoints',
                  'watchCount']
links_list = pd.read_csv('links.csv', encoding='utf-8')
links_list = links_list.sort_values(by='market_cap', ascending=False).reset_index(drop=True)

dbname = get_database()
collection_name = dbname["history_quotes"]


for token_id, link in tqdm.tqdm(links_list[['id', 'link']].values):
    session = requests.Session()

    slug = link.rsplit('/')[-1]
    print(slug)

    sg = {'slug': slug}
    data = history_quotes(int(token_id))
    data = {k.strftime("%Y-%m-%d"): v for k, v in data.items()}
    _id = {'_id': int(token_id)} | sg | data
    collection_name.update_one({'_id': int(token_id)}, {'$set': {'slug': slug}}, upsert=True)
    try:
        collection_name.insert_many([_id])
    except Exception:
        try:
            # collection_name.update_one({'_id': _id['_id']}, {'$set': update_data_ex_ts}, upsert=True)
            collection_name.update_one({'_id': _id['_id']}, {'$set': _id},
                                       upsert=True)
        except OverflowError:
            continue

"""for token_id in list(collection_name.find()):
    if len(list(collection_name.find(filter={'_id': token_id['_id']}))[0].keys()) < 3:
        collection_name.delete_one(filter={'_id': token_id['_id']})"""