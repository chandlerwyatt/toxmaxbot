from random import choice
from pprint import pprint
from argparse import ArgumentParser
from sys import exit
from datetime import datetime

from fetch import make_api_client
from mongo import make_mongo_client


def select_tweet(collection, last_user_tweeted=None):
    screen_names = collection.distinct("user.screen_name")
    if last_user_tweeted:
        screen_names.remove(last_user_tweeted)
        print(f"Last user tweeted within select_tweet: {last_user_tweeted}. Excluding them")

    selected_name = choice(screen_names)

    cur = coll.aggregate([
        {
            "$match": {
                "$and": [
                    {"favorite_count": {"$gte": 50}},
                    {"retweeted_time": {"$exists": False}},
                    {"user.screen_name": selected_name},
                ]
            }
        },
        {
            "$sample": {
                "size": 1}
        }
    ])

    chosen_tweet = cur.next()
    pprint(chosen_tweet)
    return chosen_tweet


def retweet(api, id, screen_name, collection):
    url = f"https://twitter.com/{screen_name}/status/{id}"
    api.update_status(f"Behold, the toxicity of @{screen_name} {url}")
    upd = collection.update_one(
        {"_id": id},
        {"$set": {"retweeted_time": datetime.now()}}
    )

    print("\nMatched Documents:", str(upd.matched_count))
    print("\nModified Documents:", str(upd.modified_count))
    print("\nRaw Result:\n", str(upd.raw_result))
    doc = collection.find({"id": id}).next()
    print("\nUpdated MongoDB Document:")
    pprint(doc)


def reset_bot_retweeted(collection):
    collection.update_many(
        {"retweeted_time": {"$exists": True}},
        {"$unset": {"retweeted_time": ""}})


if __name__ == '__main__':
    parser = ArgumentParser(description="Selects a tweet and retweets it")
    parser.add_argument(
                    "--reset",
                    help="Reset the bot_retweeted value on provided tweet IDs. If empty, reset on all documents",
                    action="store_true"
    )
    parser.add_argument(
        "--db",
        help="MongoDB database from which to read tweets",
        default="toxmaxbot"
    )
    parser.add_argument(
        "--collection",
        help="MongoDB collection from which to read tweets",
        default="tweets"
    )

    args = parser.parse_args()

    # call select_tweet as tweet object
    client = make_mongo_client()
    db = client.get_database(args.db)
    coll = db.get_collection(args.collection)

    if args.reset:
        reset_bot_retweeted(coll)
        print("'bot_retweeted' values have been reset.")
        exit(0)

    last_tweet = coll.find_one(sort=[('retweeted_time', -1)])
    last_user_tweeted = last_tweet['user']['screen_name'] if last_tweet else None

    print(f"Last User Tweeted: {last_user_tweeted}")

    tweet = select_tweet(collection=coll, last_user_tweeted=last_user_tweeted)
    id = tweet['id']
    screen_name = tweet['user']['screen_name']

    # create authenticated session with twitter api
    api = make_api_client()

    # retweet the selected tweet and mark it as bot_retweeted=True
    retweet(api=api, id=id, screen_name=screen_name, collection=coll)
