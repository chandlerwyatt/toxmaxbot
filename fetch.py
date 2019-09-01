from argparse import ArgumentParser
from json import dumps

import tweepy

from t import ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET
from mongo import make_mongo_client, InvalidOperation, BulkWriteError


def create_index_if_not_exist(collection, index_name):
    index_info = set(
                    map(
                        lambda x: x.rsplit("_")[0], collection.index_information().keys()))
    if index_name in index_info:
        return
    collection.create_index(index_name)


def get_tweets(api=None, screen_name=None, since_id=None):
    max_id = None
    most_recent_id = None

    while True:
        msg = f"getting tweets before {max_id}" if max_id else "getting new tweets!"
        print(msg)
        timeline = api.user_timeline(screen_name=screen_name,
                                     max_id=max_id,
                                     count=200,
                                     since_id=since_id)
        earliest_id = min(timeline, key=lambda x: x.id).id if timeline else None
        if not most_recent_id:
            most_recent_id = max(timeline, key=lambda x: x.id).id if timeline else None
        if not timeline or earliest_id == max_id or (since_id and since_id == max_id):
            print("Your application is done with yo broke ass")
            print("most_recent_id: ", most_recent_id)
            return
        for tweet in timeline:
            yield tweet
        max_id = earliest_id


def make_api_client():
    auth = tweepy.OAuthHandler(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET
    )
    auth.set_access_token(
        key=ACCESS_TOKEN_KEY,
        secret=ACCESS_TOKEN_SECRET
    )
    api = tweepy.API(auth)
    return api


def get_latest_tweets_for(screen_name, collection, twitter_api):

    last_tweet_raw = collection.aggregate(
        [
            {"$match": {"user.screen_name": screen_name}},
            {"$sort": {"id": -1}},
            {"$limit": 1},
            {"$project": {"_id": 0, "id": 1}}
        ]
    )

    last_tweet_list = list(last_tweet_raw)
    last_tweet_id = last_tweet_list[0]['id'] if len(last_tweet_list) > 0 else None

    if last_tweet_id:
        print("most_recent_id:", last_tweet_id)

    print("Getting recent tweets from:", screen_name)

    def process_tweet(tweet):
        d = dict(tweet._json)
        d['_id'] = d['id']
        return d

    try:
        collection.insert_many(
            map(process_tweet,
                filter(lambda twt:
                       not twt.text.startswith("RT @")
                       and not twt.in_reply_to_status_id,
                       get_tweets(api=twitter_api, screen_name=screen_name, since_id=last_tweet_id))
                )
        )
    except InvalidOperation:
        print("Nothing to do")

    except BulkWriteError as bwe:
        print("Bulk write error!")
        print(dumps(bwe.details))


if __name__ == "__main__":
    parser = ArgumentParser(description="Get latest tweets for one or all users")
    parser.add_argument(
        "screen_names",
        help="Screen Names of users from whom to fetch latest tweets. defaults to all users.",
        nargs="*"
    )
    parser.add_argument(
        "--db",
        help="MongoDB database in which to store tweets",
        default="toxmaxbot"
    )
    parser.add_argument(
        "--collection",
        help="MongoDB collection in which to store tweets",
        default="tweets"
    )

    args = parser.parse_args()

    api = make_api_client()
    client = make_mongo_client()
    db = client.get_database(args.db)
    collection = db.get_collection(args.collection)

    create_index_if_not_exist(collection, "user.screen_name")

    if len(args.screen_names) == 0:
        screen_names = collection.distinct("user.screen_name")
        print(screen_names)

    else:
        screen_names = args.screen_names

    for sn in screen_names:
        get_latest_tweets_for(screen_name=sn, collection=collection, twitter_api=api)
