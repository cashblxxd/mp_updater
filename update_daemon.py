import pymongo
from pprint import pprint
import secrets
from mongo_queue.queue import Queue
import time
import datetime
import dateutil.relativedelta
from ozon_api import get_postings_list, get_posting_info, get_items_ids, get_item_info, print_acts, get_item_state_rev, get_labels
import random
import string
from mongo import mark_done, get_files_list, get_file, save_file
import requests
import multiprocessing


def update_postings(api_key, client_id):
    data = client.ozon_data.postings.find_one({
        "creds": f"{api_key}:{client_id}"
    })
    if data is None:
        data = {
            "creds": f"{api_key}:{client_id}",
            "last_updated": datetime.datetime.now(),
            "data": {},
            "order_ids": {
                "all": [],
                "awaiting_packaging": [],
                "not_accepted": [],
                "arbitration": [],
                "awaiting_deliver": [],
                "delivering": [],
                "driver_pickup": [],
                "delivered": [],
                "cancelled": []
            }
        }
        last_updated = ((datetime.datetime.now() + dateutil.relativedelta.relativedelta(months=-1)).replace(day=1))
    else:
        last_updated = data["last_updated"] + dateutil.relativedelta.relativedelta(hours=-2)
    for i in data["order_ids"]:
        data["order_ids"][i] = set(data["order_ids"][i])
    neww = get_postings_list(api_key, client_id, status="ALL", since=last_updated)
    postings_add = {}
    for i in neww:
        if i["posting_number"] not in data["order_ids"]["all"]:
            print("...", i["posting_number"])
            data["order_ids"]["all"].add(i["posting_number"])
            k = get_posting_info(i, api_key, client_id)
            postings_add[i["posting_number"]] = k
            data["order_ids"][k["metadata"]["status"]].add(i["posting_number"])
        elif data[i["posting_number"]]["metadata"]["status"] != i["status"]:
            data["order_ids"][data[i["posting_number"]]["metadata"]["status"]].pop(i["posting_number"], 0)
            data[i["posting_number"]]["metadata"]["status"] = i["status"]
            data["order_ids"][data[i["posting_number"]]["metadata"]["status"]].add(i["posting_number"])
    data["data"].update(postings_add)
    for i in data["order_ids"]:
        data["order_ids"][i] = list(data["order_ids"][i])
    if "_id" in data:
        client.ozon_data.postings.update_one({
            "_id": data["_id"]
        }, {"$set": data})
    else:
        client.ozon_data.postings.insert_one(data)


def update_items(api_key, client_id):
    data = client.ozon_data.items.find_one({
        "creds": f"{api_key}:{client_id}"
    })
    if data is None:
        data = {
            "creds": f"{api_key}:{client_id}",
            "data": {},
            "ids": {
                "all": [],
                "processing": [],
                "moderating": [],
                "processed": [],
                "failed_moderation": [],
                "failed_validation": [],
                "failed": []
            }
        }
    for i in data["ids"]:
        data["ids"][i] = set(data["ids"][i])
    #pprint(data)
    print("the data current")
    neww = get_items_ids(api_key, client_id)
    #pprint(neww)
    print("got", len(neww))
    items_add = {}
    for i in neww:
        print("...", f'{i["product_id"]}:{i["offer_id"]}')
        if f'{i["product_id"]}:{i["offer_id"]}' not in data["ids"]:
            data["ids"]["all"].add(f'{i["product_id"]}:{i["offer_id"]}')
            k = get_item_info(i["product_id"], i["offer_id"], api_key, client_id)
            items_add[f'{i["product_id"]}:{i["offer_id"]}'] = k
            data["ids"][get_item_state_rev(k["Статус"])].add(f'{i["product_id"]}:{i["offer_id"]}')
        elif data["data"][f'{i["product_id"]}:{i["offer_id"]}']["Статус"] != k["Статус"]:
            k = get_item_info(i["product_id"], i["offer_id"], api_key, client_id)
            old_state, new_state = get_item_state_rev(data["data"][f'{i["product_id"]}:{i["offer_id"]}']["Статус"]), get_item_state_rev(k["Статус"])
            data["ids"][old_state].pop(f'{i["product_id"]}:{i["offer_id"]}', 0)
            data["ids"][new_state].add(f'{i["product_id"]}:{i["offer_id"]}')
    data["data"].update(items_add)
    for i in data["ids"]:
        data["ids"][i] = list(data["ids"][i])
    print("collected", len(data["data"]))
    if "_id" in data:
        client.ozon_data.items.update_one({
            "_id": data["_id"]
        }, {"$set": data})
    else:
        client.ozon_data.items.insert_one(data)
    print("updated!")


def deliver_postings(api_key, client_id, postings_numbers):
    data = client.ozon_data.postings.find_one({
        "creds": f"{api_key}:{client_id}"
    })
    if data is None:
        return False, "404"
    postings = []
    for i in postings_numbers:
        info = data["data"].get(i, 0)
        if info == 0:
            continue
        headers = {
            'Client-Id': str(client_id),
            'Api-Key': api_key,
            'Content-Type': 'application/json'
        }
        payload = {
            "packages": [{"items": info["metadata"]["products"]}],
            "posting_number": i
        }
        r = requests.post(url="http://api-seller.ozon.ru/v2/posting/fbs/ship", headers=headers, json=payload).json()
        if "result" in r:
            postings.append(i)
            data["data"]["metadata"]["status"] = "awaiting_deliver"
            try:
                data["data"]["order_ids"]["awaiting_packaging"].remove(i, 0)
            except Exception:
                pass
            data["data"]["order_ids"]["awaiting_deliver"].append(i)
    client.ozon_data.postings.update_one({
        "_id": data["_id"]
    }, {"$set": data})
    return True,


def get_test():
    flist = get_files_list("68349970-1c11-412a-a3f6-19ac61b94210", "33345")
    content = get_file(flist[list(flist.keys())[2]]["file_id"])
    with open("file.pdf", "wb") as f:
        f.write(content)


def upload_act_file(api_key, client_id):
    name, content = print_acts(api_key, client_id)
    save_file(api_key, client_id, name, content)


def upload_labels(api_key, client_id, posting_numbers):
    name, content = get_labels(api_key, client_id, posting_numbers)
    save_file(api_key, client_id, name, content)


def work(channel="postings_priority"):
    """
    :param
    channel: ["items_priority", "items_queue", "postings_priority", "postings_queue", "act_queue", "labels_queue", "deliver_queue"]
    :return: None
    """
    global client
    client = pymongo.MongoClient("mongodb+srv://dbUser:qwep-]123p=]@cluster0-ifgr4.mongodb.net/Cluster0?retryWrites=true&w=majority")
    queue = Queue(client.update_queue_db.update_queue, consumer_id=''.join(random.choice(string.ascii_lowercase) for i in range(10)), timeout=300, max_attempts=3)
    while True:
        for channel in ["items_priority", "items_queue", "postings_priority", "postings_queue", "act_queue", "labels_queue", "deliver_queue"]:
            k = queue.next(channel=channel)
            if k:
                #print(k.job_id)
                job_data = k.payload
                #print("look ma i got a job")
                pprint(job_data)
                try:
                    if channel.startswith("postings"):
                        update_postings(job_data["api_key"], job_data["client_id"])
                    elif channel.startswith("items"):
                        #print("gotta update items")
                        update_items(job_data["api_key"], job_data["client_id"])
                    elif channel.startswith("act"):
                        upload_act_file(job_data["api_key"], job_data["client_id"])
                    elif channel.startswith("labels"):
                        upload_labels(job_data["api_key"], job_data["client_id"], job_data["posting_numbers"])
                    elif channel.startswith("deliver"):
                        deliver_postings(job_data["api_key"], job_data["client_id"], job_data["posting_numbers"])
                    mark_done(job_data["job_id"])
                    k.complete()
                except Exception as e:
                    #print(e)
                    k.release()


if __name__ == "__main__":
    for channel in ["items_priority", "items_queue", "postings_priority", "postings_queue", "act_queue", "labels_queue", "deliver_queue"]:
        d = multiprocessing.Process(name=secrets.token_urlsafe(), target=work, args=(channel,))
        d.start()
        d.join()
