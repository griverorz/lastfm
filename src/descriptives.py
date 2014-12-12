#!/usr/bin/python
# -*- coding: utf-8 -
'''
@author: Gonzalo Rivero
@date Thu Nov  6 17:54:00 PST 2014
Basic descriptives of the data
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker 
from sqlalchemy import create_engine, func
from create_tables import Tracks
from time import strptime, mktime
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pymc as pm
import patsy
import theano as T

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

for i in ww: 
    plt.plot(wtimes.loc[i].index, wtimes.loc[i].value, c='grey', alpha=0.3, linewidth=1)


## Average/Median per hour
avm = wtimes.groupby(wtimes.index.labels[1]).quantile()
plt.plot(avm, c='red', linewidth=2)
plt.xlim(0, 23); plt.grid()
plt.xlabel('Time of the day')
plt.ylabel('# of songs')
plt.title('Distribution of songs per time')
plt.savefig('./../img/weekdist.pdf')


data = {'x': wtimes['value'].index.labels[1].tolist()}
sp = patsy.dmatrix('bs(x, df=4, degree=3)', data)
sp = pd.DataFrame(np.asarray(sp))


''' Model longitudinal data '''
# Parameters
with pm.Model() as pmodel:
    beta = pm.Normal('beta', mu=0, tau=1E-3, shape=sp.shape[1])

    # Model error
    eps = pm.Uniform('eps', lower=0, upper=1E4)
    track_est = T.tensor.dot(sp.values, beta)
        
    track_lk = pm.Normal('track_lk',
                         mu=track_est,
                         sd=eps,
                         observed=wtimes)

with pmodel:
    start = pm.find_MAP()
    step = pm.HamiltonianMC()
    htrace = pm.sample(5000, step, start)
    
## Burn-in period
htrace = htrace[2500:]

## Predictive posterior ##
bsmodel = lambda x, sample: np.dot(x, sample['beta'])

samples = 50
trace_pred = np.empty([samples, len(data["x"])])
for i, rand_loc in enumerate(np.random.randint(0, len(htrace), samples)):
    rand_sample = htrace[rand_loc]
    trace_pred[i] = bsmodel(sp, rand_sample)

for i in xrange(len(trace_pred)):
    plt.scatter(wtimes.index.labels[1], trace_pred[i], marker="_", c='orange')
plt.savefig('./../img/predmodel.pdf')
