from __future__ import division
import tweepy
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
import re
import textrank
import time
import matplotlib.dates as mdates


def tweet_to_db(searchQuery, start, end, tweetsPerQry=100, maxTweets=100000000):
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
    # Obtain keys
    with open('/home/ubuntu/twitter_oauth.txt') as oauth:
        keys = oauth.readlines()
    consumer_key, consumer_secret, access_token = [x.strip() for x in keys]

    # Replace the API_KEY and API_SECRET with your application's key and secret.
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    startSince = start.strftime("%Y-%m-%d")
    endUntil = (end + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    end_tweet = api.search(q=searchQuery, count=1, until=endUntil)[0]
    start_tweet = api.search(q=searchQuery, count=1, until=startSince)[0]
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
    with open('/home/ubuntu/rds_keys.txt') as rds_keys:
        keys = rds_keys.readlines()
    host, dbname, rds_user, rds_pw = [x.strip() for x in keys]

    con = psycopg2.connect(host = host, dbname = dbname, user = rds_user, password = rds_pw, port = '5432')
    cur = con.cursor()

    create_table = """
    CREATE TABLE IF NOT EXISTS tweets (id SERIAL PRIMARY KEY, tweet_id BIGINT, hashtag TEXT, 
    datetime TEXT, content TEXT);
    """
    cur.execute(create_table)
    con.commit()

    cur.execute("SELECT tweet_id FROM tweets WHERE hashtag = '%s'", searchQuery)
    if not cur.rowcount:
        unique_ids = set()
    else:
        unique_ids = set([x[0] for x in cur.fetchall()])

    insert_tweet = """
    INSERT INTO tweets(tweet_id, hashtag, datetime, content) VALUES (%s, %s, %s, %s)
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
                    tweet_list_insert = [tweet.id, searchQuery, tweet_datetime_str,
                                         unicode(tweet.text).encode('ascii', 'replace')]
                    cur.execute(insert_tweet, tweet_list_insert)
                con.commit()
            tweetCount += len(new_tweets)
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            time.sleep(180)
            continue


def tweets_db_to_pd(searchQuery, start, end):
    """
    Query SQL databased and save result into a pd dataframe.
    start and end are needed because user may search same tag w/ different
    time frame.
    """
    with open('/home/ubuntu/rds_keys.txt') as rds_keys:
        keys = rds_keys.readlines()
    host, dbname, rds_user, rds_pw = [x.strip() for x in keys]

    con = psycopg2.connect(host = host, dbname = dbname,user = rds_user, password = rds_pw, port = '5432')

    tweets_pd = pd.read_sql_query(("SELECT id, tweet_id, datetime, content FROM tweets WHERE hashtag = '%s'", searchQuery), con)
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
    tweet_pd['timegroup'] = np.floor((tweet_pd['datetime'] - start_time) / interval).astype(int)
    tweet_grouped = tweet_pd.groupby(['timegroup'])
    tweet_count = tweet_grouped['datetime'].aggregate(['count'])
    tweet_count.columns = ['count']
    tweet_count = tweet_count.reindex(range(min(tweet_count.index.values), max(tweet_count.index.values) + 1),
                                      fill_value=0)
    tweet_count['time'] = tweet_count.index.values * interval + start_time
    tweet_count['agg_tweets'] = tweet_grouped['content'].aggregate(lambda x: list(x))
    for row in tweet_count.loc[tweet_count.agg_tweets.isnull(), 'agg_tweets'].index:
        tweet_count.at[row, 'agg_tweets'] = []
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
    Returns a list of four sublists:
    peak_vals - number of tweets at peaks
    peak_time - timestamp of peaks
    peak_groups - indices of time intervals that belong to each peaks
    peak_tweets - list of lists, each sublist being a list of strings for tweets
        within one peak
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
    return [peak_vals, peak_time, peak_groups, peak_tweets]

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
    # First need to pull tweets into database
    tweet_to_db(hashtag, start, end)
    tweet_pd = tweets_db_to_pd(hashtag, start, end)
    tweet_count = group_tweets(tweet_pd)
    peak_vals, peak_time, peak_groups, peak_tweets = get_peaks(tweet_count)
    tweet_kw = textrank_analysis(peak_tweets, orig_tag = hashtag)
    return tweet_count, peak_time, peak_vals, tweet_kw

def plot_timeline(peak_vals, tweet_kw):
    """
    Plot timeline along with keywords.
    """
    sns.set_style("white")
    mpl.rcParams['axes.linewidth'] = 0
    plt.figure()
    text_alignment = {1: 'right', 0: 'left'}
    for i, kw in enumerate(tweet_kw):
        plt.plot([0, peak_vals[i] * (-1) ** i], [- 2 * i, - 2 * i], c='blue')
        plt.scatter(peak_vals[i] * (-1) ** i, - 2 * i, c='red')
        plt.text(peak_vals[i] * (-1) ** i * 0.2, - 2 * i + 1, kw, fontdict={'size': 14},
                 horizontalalignment=text_alignment[i % 2])
    plt.plot([0, 0], [- 2 * i - 1, 1], c='black')
    plt.xticks([])
    plt.yticks([])
    plt.arrow(0, 1, 0, - 2 * i, shape='full', lw=2, color = 'black',
              length_includes_head=False, head_width=8, head_length=3.8)
    plt.axis('tight')

    file_name = 'timeline.png'
    plotfile = '/home/Ubuntu/nofomo/flaskapp/flaskexample/static/' + file_name
    plt.savefig(plotfile)
    return plotfile

def plot_Ntweets(tweet_count, peak_time, peak_vals):
    """
    Plot the number of tweets through time.
    """
    sns.set_style("darkgrid")
    plt.figure()
    plt.plot(tweet_count['time'], tweet_count['count'])
    plt.xlabel('Time', fontsize=16)
    plt.ylabel('Tweets per minute', fontsize=16)
    plt.tick_params(axis='both', labelsize=12)
    x0 = mdates.date2num(min(tweet_count['time']))
    x1 = mdates.date2num(max(tweet_count['time']))
    plt.arrow(x0, 0, x1 - x0, 0, shape='full', lw=2, color='black',
              length_includes_head=False, head_width=4, head_length=0.01)

    plt.scatter(peak_time, peak_vals, c = 'red')
    file_name = 'Ntweets.png'
    plotfile = '/home/Ubuntu/nofomo/flaskapp/flaskexample/static/' + file_name
    plt.savefig(plotfile)
    return plotfile

