from flask import render_template, Flask
from flask import request
import datetime
import tweet_functions as tweet
import os.path
from celery import Celery

# Initialize Celery
app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker = app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

@celery.task
def run_analysis(hashtag, start_time, duration):
    """
    Carry out analysis for user inputs.
    Saves result to static.
    """
    start = datetime.datetime.strptime(start_time, '%Y-%m-%d-%H-%M')
    duration = datetime.timedelta(minutes = float(duration))
    end = start + duration

    # Feed inputs into tweet functions
    tweet.tweet_to_db(hashtag, start, end)
    tweet_pd = tweet.tweets_db_to_pd(hashtag, start, end)
    tweet_count = tweet.group_tweets(tweet_pd)
    # if max(tweet_count['count']) < 30: ...
    peak_vals, peak_time, peak_groups, peak_tweets = tweet.get_peaks(tweet_count)
    tweet_kw = textrank_analysis(peak_tweets, orig_tag=hashtag)
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
      run_analysis(hashtag, start_time, duration)
      return render_template("whats_missed_delay.html")
