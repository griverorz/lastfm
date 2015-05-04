#!/usr/bin/python
# -*- coding: utf-8 -
'''
@author: Gonzalo Rivero
@date Thu Nov  6 17:54:00 PST 2014
Basic descriptives of the data
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker 
from sqlalchemy import create_engine, func, engine
from create_tables import Tracks
from time import strptime, mktime
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

dbdata = {'drivername': 'postgres',
          'host': 'localhost',
          'port': '5432',
          'username': 'gonzalorivero',
          'password': 'root',
          'database': 'lastfm'}


DBase = declarative_base()
engine = create_engine(engine.url.URL(**dbdata))
DBase.metadata.bind = engine
DBSession = sessionmaker()
session = DBSession(bind=engine)


def sqltracks(user):
    ''' Query to get track data from a give user '''
    ast = session.query(func.to_char(
        func.to_timestamp(Tracks.timestamp), 'HH24 DD-MM-YYYY').label('t')
    ).filter(Tracks.username==user).subquery('ast')
    query = session.query(ast.c.t, func.count(ast.c.t).label('count')
    ).group_by(ast.c.t).order_by(ast.c.t)
    return query


query = sqltracks('juegologo')
times, value = zip(*[(strptime(i, '%H %d-%m-%Y'), int(j)) for i, j in query])
times = [datetime.fromtimestamp(mktime(i)) for i in times]
value = [y for (x, y) in sorted(zip(times, value))]
times.sort()


times = pd.DataFrame({'times':times, 'value':value})
times = times[times.times > "2013-07-01"]
dtimes = times.groupby(
    times.times.map(lambda x: (x.year, x.month, x.day))).aggregate(np.sum)
dtimes.index = dtimes.index.map(lambda x: datetime(*x))


plt.figure(figsize=(12, 6))
plt.plot(dtimes.index, dtimes.value, c='red', lw=1)
plt.title('Music listened by hour')
plt.ylabel('# of tracks'); plt.grid()
plt.savefig('./../img/ntracks.png')
 
# Complete gaps in the index
times.index = times.times
times = times.reindex(pd.date_range(min(times.times), max(times.times), freq='H'), fill_value=0)
times.times = times.index

'''
Get data for a given periodicity
Get tracks per hour, grouped by day (not enough counts per day)
'''
wtimes = times.groupby([times.times.map(lambda x: (x.year, x.month, x.week)), 
                        times.times.map(lambda x: x.hour)]).aggregate(np.sum)
wtimes.index.set_names(['day', 'hour'], inplace=True)


# Plot data looping over weeks (since moved to SF)
ww = wtimes.index.levels[0]
init = (2013, 7)
ww = filter(lambda x: x > init, ww)

plt.figure(figsize=(12, 6))
for i in ww: 
    plt.plot(wtimes.loc[i].index, wtimes.loc[i].value, c='grey', alpha=0.3, linewidth=1)


## Average/Median per hour
avm = wtimes.groupby(wtimes.index.labels[1]).quantile()
plt.plot(avm, c='red', linewidth=2)
plt.xlim(0, 23); plt.grid()
plt.xlabel('Time of the day')
plt.ylabel('# of songs')
plt.title('Distribution of songs per hour')
plt.savefig('./../img/weekdist.png')
