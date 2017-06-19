from __future__ import division
import tweepy
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import datetime
import time
import seaborn as sns
import matplotlib.pyplot as plt
import re
import textrank
import time
import os

def tweet_to_db(searchQuery, start, end, tweetsPerQry=100, maxTweets=100000000,
                dbname=None):
    """
    This function pulls tweets with hashtag searchQuery from a specified time period.
    tweetsPerQry = 100 is the maximal number of tweets allowed by Twitter per query.
    maxTweets is some arbitrary big number.
    Tweets are instantly inserted into an SQL database with one table 'tweets',
    which has the following 4 columns:
    id - unique identifier
    tweet_id - BIGINT, unique tweet id
    datetime - TEXT, datetime in format '%Y-%m-%d %H:%M:%S'
    content - TEXT, tweet content

    """
    startSince = start.strftime("%Y-%m-%d")
    endUntil = (end + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    end_tweet = api.search(q=searchQuery, count=1, until=endUntil)[0]
    start_tweet = api.search(q=searchQuery, count=1, until=startSince)[0]
    max_id, min_id = end_tweet.id, start_tweet.id
    tweetCount = 0

    # Identify ending id of tweets within timeframe using a binary search
    # This unfortunately is not available directly through API
    while end_tweet.created_at - end > datetime.timedelta(0, 5, 0):
        mid_id = int((end_tweet.id + start_tweet.id) / 2)
        # Grab 10 tweets just to make sure they are not all zeros
        mid_tweet = api.search(q=searchQuery, count=10, max_id=mid_id)[0]
        if end - mid_tweet.created_at > datetime.timedelta(0, 5, 0):
            start_tweet = mid_tweet
        else:
            end_tweet = mid_tweet

    max_id = end_tweet.id

    # Create db to store results
    if dbname is None:
        dbname = searchQuery.strip('#') + '_db'
    username = 'xiaoxiao'
    engine = create_engine('postgres://%s@localhost/%s' % (username, dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    con = psycopg2.connect(database=dbname, user=username)
    cur = con.cursor()
    cur.execute("SELECT * FROM information_schema.tables where table_name='tweets'")
    if bool(cur.rowcount):  # If db already exists: resume from previous breaking point
        cur.execute("SELECT tweet_id FROM tweets")
        unique_ids = set([x[0] for x in cur.fetchall()])

    else:
        unique_ids = set()
        create_table = """
        CREATE TABLE IF NOT EXISTS tweets (id SERIAL PRIMARY KEY, tweet_id BIGINT, 
        datetime TEXT, content TEXT);
        """

        cur.execute(create_table)
        con.commit()

    insert_tweet = """
    INSERT INTO tweets(tweet_id, datetime, content) VALUES (%s, %s, %s)
    """

    while tweetCount < maxTweets:
        try:
            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang='en',
                                    max_id=str(max_id - 1), since=startSince,
                                    until=endUntil)

            if not new_tweets:
                print("No more tweets found")
                break
            if new_tweets[-1].created_at < start:
                print("Exhausted time interval.")
                break
            for tweet in new_tweets:
                if tweet.id not in unique_ids:
                    unique_ids.add(tweet.id)
                    tweet_datetime = tweet.created_at
                    tweet_datetime_str = tweet_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    tweet_list_insert = [tweet.id, tweet_datetime_str,
                                         unicode(tweet.text).encode('ascii', 'replace')]
                    cur.execute(insert_tweet, tweet_list_insert)
                con.commit()
            tweetCount += len(new_tweets)
            print("Downloaded {0} tweets".format(tweetCount))
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            print "Time out error caught."
            time.sleep(180)
            continue

    print ("Downloaded {0} tweets in total.".format(tweetCount))

def tweets_db_to_pd(dbname, start, end):
    """
    Query SQL databased and save result into a pd dataframe.
    start and end are needed because user may search same tag w/ different
    time frame.
    """
    username = 'xiaoxiao'
    con = psycopg2.connect(database = dbname, user = username)
    sql_query = """
    SELECT * FROM tweets;
    """
    tweets_pd = pd.read_sql_query(sql_query, con)
    tweets_pd['datetime'] = tweets_pd['datetime'].apply(lambda y: datetime.datetime.strptime(y, '%Y-%m-%d %H:%M:%S'))
    tweets_pd_rows = [start <= tweets_pd['datetime'][i] <= end for i in range(len(tweets_pd))]
    tweets_pd = tweets_pd.loc[tweets_pd_rows, :]
    return tweets_pd

def group_tweets(tweet_pd, interval = datetime.timedelta(0, 1, 0)):
    """
    Group tweets by time intervals.
    Returns a pd dataframe with three columns:
    time, number of tweets, all tweets within interval in a list
    """
    start_time = min(tweet_pd['datetime'])
    end_time = max(tweet_pd['datetime'])
    duration = end_time - start_time
    tweet_pd['timegroup'] = np.floor((tweet_pd['datetime'] - start_time) / interval).astype(int)
    tweet_grouped = tweet_pd.groupby(['timegroup'])
    tweet_count = tweet_grouped['datetime'].aggregate(['count'])
    tweet_count.columns = ['count']
    tweet_count['time'] = tweet_count.index.values * interval + start_time
    tweet_count['agg_tweets'] = tweet_grouped['content'].aggregate(lambda x: list(x))
    return tweet_count

def peakdet(v, delta):
    """
    Maxima detection function from https://gist.github.com/endolith/250860
    Converted from MATLAB script at http://billauer.co.il/peakdet.html

    Returns two arrays

    function [maxtab, mintab]=peakdet(v, delta, x)
    %PEAKDET Detect peaks in a vector
    %        [MAXTAB, MINTAB] = PEAKDET(V, DELTA) finds the local
    %        maxima and minima ("peaks") in the vector V.
    %        MAXTAB and MINTAB consists of two columns. Column 1
    %        contains indices in V, and column 2 the found values.
    %
    %        With [MAXTAB, MINTAB] = PEAKDET(V, DELTA, X) the indices
    %        in MAXTAB and MINTAB are replaced with the corresponding
    %        X-values.
    %
    %        A point is considered a maximum peak if it has the maximal
    %        value, and was preceded (to the left) by a value lower by
    %        DELTA.

    % Eli Billauer, 3.4.05 (Explicitly not copyrighted).
    % This function is released to the public domain; Any use is allowed.

    """
    maxtab = []
    mintab = []

    x = np.arange(len(v))

    v = np.asarray(v)

    mn, mx = np.Inf, -np.Inf
    mnpos, mxpos = np.NaN, np.NaN

    lookformax = True

    for i in np.arange(len(v)):
        this = v[i]
        if this > mx:
            mx = this
            mxpos = x[i]
        if this < mn:
            mn = this
            mnpos = x[i]

        if lookformax:
            if this < mx - delta:
                maxtab.append((mxpos, mx))
                mn = this
                mnpos = x[i]
                lookformax = False
        else:
            if this > mn + delta:
                mintab.append((mnpos, mn))
                mx = this
                mxpos = x[i]
                lookformax = True

    return np.array(maxtab), np.array(mintab)

def get_peaks(tweet_count, delta=0.25):
    """
    Returns a list of five sublists:
    peak_vals - number of tweets at peaks
    peak_time - timestamp of peaks
    peak_groups - indices of time intervals that belong to each peaks
    peak_tweets - list of lists, each sublist being a list of strings for tweets
        within one peak
    nonpeak_tweets - tweets from tweet_count that do not belong to any of the peaks
    """
    sig = peakdet(tweet_count['count'], max(tweet_count['count']) * delta)
    peaks = sig[0]
    loc = [x[0] for x in peaks]
    peak_vals = [x[1] for x in peaks]
    peak_time = tweet_count.ix[loc, 'time'].values
    # Define each peak as non-increasing towards both sides
    peak_groups, peak_tweets = [], []
    for single_loc in loc:
        single_group = [single_loc]
        tweet_group = tweet_count.ix[single_loc, 'agg_tweets']
        i, j = single_loc - 1, single_loc + 1
        while tweet_count.ix[i, 'count'] <= tweet_count.ix[i + 1, 'count'] and i >= 0:
            single_group.extend([i])
            tweet_group += tweet_count.ix[i, 'agg_tweets']
            i -= 1
        while tweet_count.ix[j, 'count'] <= tweet_count.ix[j - 1, 'count'] \
                and j < len(tweet_count.index):
            single_group.extend([j])
            tweet_group += tweet_count.ix[j, 'agg_tweets']
            j += 1
        peak_groups.append(single_group)
        peak_tweets.append(tweet_group)

    nonpeak_rows = [row for row in range(len(tweet_count)) if
                    not any(row in sublist for sublist in peak_groups)]
    nonpeak_tweets = [tweet_count.ix[row, 'agg_tweets'] for row in nonpeak_rows]
    nonpeak_tweets = [' '.join(x) for x in nonpeak_tweets]
    return [peak_vals, peak_time, peak_groups, peak_tweets, nonpeak_tweets]

def clean_tweet(tweet_string, hashtag):
    """
    Clean a string of tweet(s) by removing original hashtag, urls, @, RT, and newlines
    """
    cleaned_tweet = re.sub(r"http\S+", "", tweet_string)
    cleaned_tweet = re.sub(r"@\S+", "", cleaned_tweet)
    cleaned_tweet = cleaned_tweet.replace(hashtag, '').replace('RT', '').replace('\n', ' ').replace('\r', '')
    return cleaned_tweet

def textrank_analysis(peak_tweets, orig_tag = ''):
    """
    Takes a list of lists of tweets, with each sublist being tweets within a peak
    Concatenate & clean tweets.
    Returns a list of #1 keyword for each sublist determined by TextRank.
    """
    textrank_kw = []
    for peak in peak_tweets:
        text = '.'.join(peak)
        text = clean_tweet(text, orig_tag)
        peak_textrank = textrank.extractKeyphrases(text)
        textrank_keys = sorted(peak_textrank.items(), key = lambda x: x[1], reverse = True)[0][0]
        textrank_kw.append(textrank_keys)
    return textrank_kw

def overall_analysis(hashtag, start, end):
    """
    Overarching function that carries out analysis.

    """
    dbname = hashtag.strip('#') + '_db'
    tweet_pd = tweets_db_to_pd(dbname, start, end)
    tweet_count = group_tweets(tweet_pd)
    peak_vals, peak_time, peak_groups, peak_tweets, nonpeak_tweets = get_peaks(tweet_count)
    tweet_kw = textrank_analysis(peak_tweets, orig_tag = hashtag)
    return tweet_count, peak_time, peak_vals, tweet_kw

def plot_timeline(peak_vals, tweet_kw, resolution = 1000):
    """
    Plot timeline along with keywords.
    """
    plt.figure()
    for i, kw in enumerate(tweet_kw):
        plt.plot([i, i], [0, peak_vals[i] * (-1) ** i], c = 'blue')
        plt.scatter(i, peak_vals[i] * (-1) ** i, c = 'red')
        plt.text(i, peak_vals[i] * (-1) ** i * 1.1, kw, fontdict={'size': 8})
    plt.plot([-1, i + 1], [0, 0], c = 'black')
    plt.xticks([])
    plt.yticks([])

    file_name = 'timeline.png'
    plotfile = '/Users/xiaoxiao/Documents/GitHub/insight/flaskapp/flaskexample/static/' + file_name
    plt.savefig(plotfile)
    return '../static/' + file_name

def plot_Ntweets(tweet_count, peak_time, peak_vals, resolution = 500):
    """
    Plot the number of tweets through time.
    """
    plt.figure()
    plt.plot(tweet_count['time'], tweet_count['count'])
    plt.scatter(peak_time, peak_vals, c = 'red')
    file_name = 'Ntweets.png'
    plotfile = '/Users/xiaoxiao/Documents/GitHub/insight/flaskapp/flaskexample/static/' + file_name
    plt.savefig(plotfile)
    return '../static/' + file_name

# Obtain keys
with open('/Users/xiaoxiao/Documents/GitHub/insight/flaskapp/flaskexample/twitter_oauth.txt') as oauth:
    keys =  oauth.readlines()
consumer_key, consumer_secret, access_token = [x.strip() for x in keys]

# Replace the API_KEY and API_SECRET with your application's key and secret.
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)
