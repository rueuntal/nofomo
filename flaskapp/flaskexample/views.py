from flask import render_template, Flask
from flaskexample import app
from flask import request
import datetime
import tweet_functions as tweet
import os.path

@app.route('/')
@app.route('/index')
def whats_missed_input():
    return render_template("whats_missed_input.html")

@app.route('/example_comeyday')
def whats_missed_comey():
    return render_template("whats_missed_comeyday.html")

@app.route('/example_NBAFinals')
def whats_missed_nbafinals():
    return render_template("whats_missed_NBAFinals.html")

@app.route('/output')
def whats_missed_output():
  # Pull the three pieces of inputs
  hashtag = request.args.get('hashtag')
  start_time = str(request.args.get('start_time'))
  ntweet_plot_dir = '/home/Ubuntu/nofomo/flaskapp/flaskexample/static/' + 'Ntweets_' + \
                    hashtag.strip('#') + '_' + start_time + '.png'
  timeline_plot_dir = '/home/Ubuntu/nofomo/flaskapp/flaskexample/static/' + 'timeline_' + \
                    hashtag.strip('#') + '_' + start_time + '.png'
  if os.path.isfile(ntweet_plot_dir):
      return render_template("whats_missed_output.html", hashtag = hashtag, ntweet_plot = ntweet_plot_dir,
                             timeline_plot = timeline_plot_dir)
  else:
      return render_template("whats_missed_input.html")
  # start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d-%H-%M')
  # duration = datetime.timedelta(minutes = float(request.args.get('duration')))
  # end_time = start_time + duration

  # # Feed inputs into main functions
  # tweet_count, peak_time, peak_vals, tweet_kw = tweet.overall_analysis(hashtag, start_time, end_time)
  # ntweet_plot = tweet.plot_Ntweets(tweet_count, peak_time, peak_vals)
  # timeline_plot = tweet.plot_timeline(peak_vals, tweet_kw)
  # return render_template("whats_missed_output.html", hashtag = hashtag, ntweet_plot = ntweet_plot,
  #                        timeline_plot = timeline_plot)