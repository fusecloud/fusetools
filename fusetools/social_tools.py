import pandas as pd
import tweepy
import os


class Twitter:

    @classmethod
    def pull_user_likes(cls, screen_name, twtr_api_key, twtr_api_secret, count=100):
        # TWITTER AUTH
        print("Authenticating to Twitter")

        screen_name = screen_name
        auth = tweepy.AppAuthHandler(twtr_api_key, twtr_api_secret)
        api = tweepy.API(auth)
        alltweets = api.favorites(screen_name=screen_name, count=count)

        tweet_df = \
            pd.DataFrame({
                "id": [x._json.get('id') for x in alltweets],
                "datetime": [x._json.get('created_at') for x in alltweets],
                "text": [x._json.get('text') for x in alltweets],
                "tweet_source": [x._json.get('source') for x in alltweets],
                "symbols": [x._json.get('entities').get('symbols')
                            for x in alltweets],
                "rt_count": [x._json.get('retweet_count') for x in alltweets],
                "fav_count": [x._json.get('favorite_count') for x in alltweets]
            })

        return tweet_df

    @classmethod
    def pull_user_tweets(cls, screen_name, twtr_api_key, twtr_api_secret):
        # TWITTER AUTH
        print("Authenticating to Twitter")

        screen_name = screen_name
        auth = tweepy.AppAuthHandler(twtr_api_key, twtr_api_secret)
        api = tweepy.API(auth)

        # initialize a list to hold all the tweepy Tweets
        alltweets = []

        print(f"Grabbing user: {screen_name} tweets")
        # make initial request for most recent tweets (200 is the maximum allowed count)
        new_tweets = api.user_timeline(screen_name=screen_name, count=200)

        # save most recent tweets
        alltweets.extend(new_tweets)

        # save the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

        # keep grabbing tweets until there are no tweets left to grab
        while len(new_tweets) > 0:
            print(f"getting tweets before {oldest}")

            # all subsequent requests use the max_id param to prevent duplicates
            new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)

            # save most recent tweets
            alltweets.extend(new_tweets)

            # update the id of the oldest tweet less one
            oldest = alltweets[-1].id - 1

            print(f"...{len(alltweets)} tweets downloaded so far")

        tweet_df = \
            pd.DataFrame({
                "id": [x._json.get('id') for x in alltweets],
                "datetime": [x._json.get('created_at') for x in alltweets],
                "text": [x._json.get('text') for x in alltweets],
                "tweet_source": [x._json.get('source') for x in alltweets],
                "symbols": [x._json.get('entities').get('symbols')
                            for x in alltweets],
                "media": [x._json.get('entities').get('media')
                          for x in alltweets],
                "rt_count": [x._json.get('retweet_count') for x in alltweets],
                "fav_count": [x._json.get('favorite_count') for x in alltweets]
            })

        print(f"len of tweets: {len(tweet_df)}")
        tweet_df = tweet_df[tweet_df['symbols'].astype(str).str.contains("text")].reset_index(drop=True)

        tweet_df['symbols_flat'] = \
            (tweet_df
             # .apply(lambdas x: flatten_dicts(x['symbols']), axis=1)
             .apply(lambda x: [d.get("text") for d in x['symbols']], axis=1)
             .astype(str)
             .str.replace("[", "")
             .str.replace("]", "")
             .str.replace("'", "")
             )

        tweet_df['tweet_source'] = \
            tweet_df.apply(lambda x: (x['tweet_source']
                .split(">")[1]
                .split("<")[0]
                ), axis=1)

        tweet_df['datetime'] = \
            pd.to_datetime(tweet_df['datetime'])  # .dt.tz_localize('UTC')

        # tweet_df['datetime'] = \
        #     tweet_df['datetime'].dt.tz_convert('US/Eastern')

        tweet_df['datetime'] = \
            tweet_df['datetime'].astype(str).str[:19]

        tweet_df['tweet_link'] = \
            tweet_df.apply(
                lambda x: "https://twitter.com/" + \
                          screen_name +
                          "/status/" + \
                          str(x['id']), axis=1)

        tweet_df.drop(["symbols"], axis=1, inplace=True)
        tweet_df.rename(columns={"symbols_flat": "symbols"}, inplace=True)
        return tweet_df

    @classmethod
    def pull_tweet_details(cls, twtr_api_key, twtr_api_secret, tweet_id):
        auth = tweepy.AppAuthHandler(twtr_api_key, twtr_api_secret)
        api = tweepy.API(auth)
        tweet = api.get_status(tweet_id)
        return tweet
