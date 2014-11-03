# -*- coding:utf-8 -*-
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdt
import enum
from collections import OrderedDict as cod
import plotly.plotly as plplt
from plotly.graph_objs import *
import ConfigParser

class statics_plot():

  Type = enum.Enum('Type', 'line plot bar');
  config_file = 'plotly.cfg'
  target = 'usinghtc'

  default_config = {
      'user': 'null',
      'api_key': 'null',
  }

  def __init__(self):
      self.set_config()
      self.do_oauth()

  def __del__(self):
      pass

  def set_config(self):
    try:
      self.config = ConfigParser.SafeConfigParser(self.default_config)
      self.config.read(self.config_file)
      if not self.config.has_section(self.target):
        self.config.add_section(self.target)

      self.user = self.config.get(self.target, 'user')
      self.api_key = self.config.get(self.target, 'api_key')
    except:
      print("Could not read config file : %s" % (self.config_file))

  def do_oauth(self):
    plplt.sign_in(self.user, self.api_key)

  # Format of item in items is dictionary, key:date value:data
  def generate_statics_image(self, items, since, latest, days=1, type=Type.line, folder='images/', color='r'):
    x_label, statics = self.generate_sum(items, since, latest, days=days)
    statics = cod(sorted(statics.items()))

    fig = plt.figure()
    graph = fig.add_subplot(111)
    # plot type
    if type==self.Type.line:
        graph.plot(x_label, list(statics.values()), color=color)
    elif type==self.Type.plot:
        graph.plot(x_label, list(statics.values()), 'o', color=color)
    elif type==self.Type.bar:
        graph.bar(x_label, list(statics.values()), color=color, edgecolor='k')
    else:
        print("Error: "+type+" does not match any of Type enum.")

    # graph format for days
    days = mdt.DayLocator(interval=days) # every day
    daysFormat = mdt.DateFormatter('%Y-%m-%d')
    graph.xaxis.set_major_locator(days)
    graph.xaxis.set_major_formatter(daysFormat)
    fig.autofmt_xdate()

    # Generate graph image file
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    file_path = folder+now+'.png'
    plt.savefig(file_path)

    return file_path

  def generate_statics_plotly(self, items, since, latest, days=1, file=''):
    x_label, statics = self.generate_sum(items, since, latest, days=days)
    statics = cod(sorted(statics.items()))
    data = Data([
        Scatter(
            x=x_label,
            y=statics.values()
        )
    ])
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = now + '_' + file
    plot_url = plplt.plot(data, filename=file_name)
    return plot_url

  def generate_sum(self, items, since, latest, days=1):
    x_label = self.generate_date_label(since, latest, days=days)
    return list(reversed(x_label[1:])), {x_label[i]:len([k for k,v in items.items() if x_label[i] < k and k < x_label[i-1]]) for i in range(len(x_label)-1, 0, -1)}

  def generate_date_label(self, since, latest, days=1):
    return [latest - timedelta(days=x) for x in range(0, (latest-since).days, days)]
