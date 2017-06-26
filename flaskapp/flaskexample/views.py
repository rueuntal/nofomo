from flask import render_template, Flask, request, copy_current_request_context
from flaskexample import app
import datetime
import tweet_functions as tweet
import os.path
import os
from celery import Celery
import sys

# Initialize Celery
with open('/home/ubuntu/nofomo/flaskapp/mq.txt') as oauth:
    keys = oauth.readlines()
mqdir = keys[0].strip()
redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379/0')
app.config['CELERY_BROKER_URL'] = mqdir
celery = Celery(app.name, broker = mqdir)
celery.conf.update(app.config)

@celery.task
def foo():
    print "success!" > open('../test.txt', 'a')

@celery.task
def run_analysis(hashtag, start_time, duration):
    """
    Carry out analysis for user inputs.
    Saves result to static.
    """
    print "Enter run_analysis function."
    sys.stdout.flush()
    start = datetime.datetime.strptime(start_time, '%Y-%m-%d-%H-%M')
    duration = datetime.timedelta(minutes = float(duration))
    end = start + duration

    # Feed inputs into tweet functions
    #tweet.tweet_to_db(hashtag, start, end)
    print "Pull tweets from db"
    sys.stdout.flush()
    tweet_pd = tweet.tweets_db_to_pd(hashtag, start, end)
    print "count tweets"
    sys.stdout.flush()
    tweet_count = tweet.group_tweets(tweet_pd)
    # if max(tweet_count['count']) < 30: ...
    print "get peaks"
    sys.stdout.flush()
    peak_vals, peak_time, peak_groups, peak_tweets = tweet.get_peaks(tweet_count)
    print "get keywords"
    sys.stdout.flush()
    tweet_kw = tweet.textrank_analysis(peak_tweets, orig_tag=hashtag)
    print "try to plot"
    sys.stdout.flush()
    tweet.plot_timeline(peak_vals, tweet_kw, hashtag, start_time)
    tweet.plot_Ntweets(tweet_count, peak_time, peak_vals, hashtag, start_time)

@app.route('/')
@app.route('/index')
def whats_missed_input():
    return render_template("whats_missed_input.html")

@app.route('/example_comeyday')
def whats_missed_comey():
    return render_template("whats_missed_output.html", hashtag='#comeyday', ntweet_plot='../static/Ntweets_comeyday_2017-06-08-14-00.png',
                           timeline_plot='../static/timeline_comeyday_2017-06-08-14-00.png')


@app.route('/example_NBAFinals_2')
def whats_missed_nbafinals():
    return render_template("whats_missed_output.html", hashtag='#NBAFinals', ntweet_plot='../static/Ntweets_NBAFinals_2017-06-13-01-00.png',
                           timeline_plot='../static/timeline_NBAFinals_2017-06-13-01-00.png')

@app.route('/output')
def whats_missed_output():
  # Pull the three pieces of inputs
  hashtag = request.args.get('hashtag')
  start_time = str(request.args.get('start_time'))
  duration = request.args.get('duration')
  ntweet_file = 'Ntweets_' + hashtag.strip('#') + '_' + start_time + '.png'
  timeline_file = 'timeline_' + hashtag.strip('#') + '_' + start_time + '.png'
  ntweet_plot_dir = '/home/ubuntu/nofomo/flaskapp/flaskexample/static/' + ntweet_file
  if os.path.isfile(ntweet_plot_dir):
      return render_template("whats_missed_output.html", hashtag = hashtag, ntweet_plot = '../static/' + ntweet_file,
                             timeline_plot = '../static/' + timeline_file)
  else:
      print "Enter else branch."
      #run_analysis.apply_async([hashtag, start_time, duration], countdown = 10)
      foo.delay()
      return render_template("whats_missed_delay.html")
