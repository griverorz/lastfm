#!/usr/bin/python

# -*- coding: utf-8 -*-

'''
@author: Gonzalo Rivero
@date Sat Nov  8 19:23:57 PST 2014
Two-state model
'''

import pandas as pd
import psycopg2 as pg
import numpy as np
import pymc as pm
import theano as T

conn = pg.connect(database='lastfm', 
                  host='127.0.01',
                  user='gonzalorivero', 
                  password='root', 
                  port='5432')

cur = conn.cursor()
f = open('tagdata.sql', 'r')
cur.execute(f.read())
tagdata = cur.fetchall()
colnames = [desc[0] for desc in cur.description]
tagdata = pd.DataFrame(tagdata, columns=colnames)

tagdata['prop'] = tagdata.groupby(tagdata.day).counts.apply(lambda x: x/np.sum(x))

sels = tagdata.groupby(tagdata.tag).prop.max() 
sels = sels.index[sels > .25].tolist()
tagdata = tagdata[tagdata.tag.map(lambda x: x in sels)]

tagdata = tagdata.groupby(tagdata.day).apply(lambda x: x.groupby(tagdata.tag).sum())
tagdata.reset_index(level=[0,1], inplace=True)

fulldist = pd.DataFrame({'tag':list(set(tagdata.tag)) * len(set(tagdata.day)), 
                         'day':list(set(tagdata.day)) * len(set(tagdata.tag))})

fulldist = pd.merge(fulldist, tagdata, on=['day', 'tag'], how='outer')
fulldist.loc[np.isnan(fulldist.prop), "prop"] = 0
fulldist.loc[np.isnan(fulldist.counts), "counts"] = 0

# fulldist = fulldist.iloc[0:100]
mtag = [list(fulldist.loc[fulldist.day == i, "prop"]) for i in set(fulldist.day)]
day = list(set(fulldist.day))

with pm.Model() as per_model:
    ''' Model underlying states '''
    state = pm.Bernoulli('state', p=0.5, shape=len(day))

    # Parameters
    alpha = pm.Normal('alpha', mu=0, tau=1E-3, shape=len(mtag[0]))
    beta = pm.Normal('beta', mu=0, tau=1E-3, shape=len(mtag[0]))
    
    ## Softmax
    def invlogit(x):
        return T.tensor.nnet.softmax(x)

    theta = np.empty(len(day), object)
    p_vec = np.empty(len(day), object)
    track_lk = np.empty(len(day), object)

    ## empty theta
    for i, j in enumerate(day):
        theta[i] = T.dot(state[i], beta.T)
        p_vec[i] = invlogit(theta[i])

        # Data likelihood
        track_lk[i] = pm.Dirichlet('track_lk',
                                   a=p_vec[i], shape=len(mtag[0]),
                                   observed=mtag[i])
        
with per_model:
    # start = pm.find_MAP()
    step = pm.Metropolis()
    nsteps = 1000
    trace = pm.sample(nsteps, step)
