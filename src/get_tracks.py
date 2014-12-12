#!/usr/bin/python

# -*- coding: utf-8 -*-

'''
@author: Gonzalo Rivero
@date Thu Oct 30 20:24:47 PDT 2014
Extract personal information from Last.fm
'''

import pylast
import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, exists
from create_tables import Artists, ArtistTags, Tracks
import getopt
import sys
import time

# API keys provided by last.fm
class ExtractMusic:

    def __init__(self):
        self.db = "postgresql://gonzalorivero:root@localhost:5432/lastfm"


    def auth(self, credfile):
        f = open(credfile).read()
        creds = json.loads(f)
        pwd = pylast.md5(creds["pwd"])
        try:
            network = pylast.LastFMNetwork(api_key=creds["key"],
                                           api_secret=creds["secret"], 
                                           username=creds["uname"], 
                                           password_hash=pwd)
            self.network = network
        except:
            print 'Connection to last.fm failed!'
        else:
            print 'Connection to last.fm succeeded!'


    def get_user(self, uname):
        self.user = self.network.get_user(uname)


    def get_info(self, obj):
        '''
        Common way to get info for all objects
        '''
        def _parse_tags(out):
            out = [(i.item.name, int(i.weight)) for i in out]
            return out

        d = dict.fromkeys(["element", "tags", "playcount", "listcount"])
        d["element"] = obj.get_name()
        d["mbid"] = obj.get_mbid()
        d["listcount"] = obj.get_listener_count()
        d["playcount"] = obj.get_playcount()
        d["tags"] = _parse_tags(obj.get_top_tags())
        d["username"] = self.user.get_name()
        return d


    def get_artists(self, limit):
        '''
        Get list of listened artists 
        Stores the info temporarily
        '''
        ulib = self.user.get_library()
        alist = ulib.get_artists(limit=limit)

        self.artist_list = alist

        dartist = dict.fromkeys(["artist", "playcount"])
        dartist["artist"] = [i.item.get_name() for i in alist]
        dartist["playcount"] = [i.playcount for i in alist]
        self.artists = dartist

    def artist_info(self, artist):
        out = self.get_info(self.network.get_artist(artist))
        return out


    def write_artist(self, artist):
        aartist = {i: artist[i] for i in artist.keys() if i != 'tags'}
        (ret, ), = self.session.query(exists().where(
            Artists.element != aartist['element'])
        )
        if ret:
            new_artist = Artists(**aartist)
            self.session.add(new_artist)
            self.session.commit()
            # self.session.flush()  

            atags = artist['tags']
            if atags:
                for i in xrange(len(atags)):

                    new_atag = ArtistTags(tag=atags[i][0],
                                          weight=atags[i][1], 
                                          artist_id=new_artist.id)
                    self.session.add(new_atag)
                    self.session.commit()


    def artist_tracks(self, artist):
        tracks = self.user.get_artist_tracks(artist)

        def _parse_track(tt):
            ptrack = dict.fromkeys(["element", "song", "album", "timestamp"])
            ptrack["element"] = tt.track.get_artist().get_name()
            ptrack["song"] = tt.track.get_name()
            ptrack["timestamp"] = int(tt.timestamp)
            ptrack["album"] = tt.album
            ptrack["username"] = self.user.get_name()
            return ptrack 

        # def _combine_tracks(tracks):
        #     out = dict.fromkeys(["artist", "song", "album", "timestamp"])
        #     for k in out.keys():
        #         out[k] = [i[k] for i in tracks]
        #     return out
    
        artist_tracks = map(_parse_track, tracks)
        return artist_tracks


    def write_artist_tracks(self, tracklist):
        for track in tracklist:
            new_track = Tracks(**track)
            self.session.add(new_track)
            self.session.commit()
    

    # def get_track_info(self, artist, song):
    #     track = self.network.get_track(artist, song)
    #     album = track.get_album()
    #     trackinfo = self.get_info(track)
    #     albuminfo = self.get_info(album)
    #     return trackinfo, albuminfo


    def connect_sql(self):
        DBase = declarative_base()
        self.engine = create_engine(self.db)
        DBase.metadata.bind = self.engine
        DBSession = sessionmaker()
        self.session = DBSession(bind=self.engine)


def get_music(LFMusic):
    LFMusic.connect_sql()
    t = 0
    Tt = len(LFMusic.artists["artist"])
    for artist in LFMusic.artists["artist"]:
        print 'Parsing {} out of {}'.format(t, Tt)
        try:
            ainfo = LFMusic.artist_info(artist)
            LFMusic.write_artist(ainfo)
            atracks = LFMusic.artist_tracks(artist)
            LFMusic.write_artist_tracks(atracks)
        except:
            print "\tParsing {} failed".format(artist.encode('utf-8'))
        time.sleep(2.5)
        t += 1

        
def main(argv):
    ## Defaults
    creds = './credentials.json'
    uname = 'juegologo'
    alimit = 7500
    try:
        opts, args = getopt.getopt(argv, 'c:u:l')
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-c':
            creds = str(arg)
        if opt == '-u':
            uname = str(arg)
        if opt == '-l':
            alimit = int(arg)
    
    musicdata = ExtractMusic()
    musicdata.auth(creds)
    musicdata.get_user(uname)
    musicdata.get_artists(alimit)
    get_music(musicdata)
            


if __name__ == '__main__':
    main(sys.argv[1:])
