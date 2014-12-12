#!/usr/bin/python

'''
@author: Gonzalo Rivero
@date Thu Oct 30 20:24:47 PDT 2014
Create tables for storing lastfm data
'''

from sqlalchemy import create_engine, Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base


DBase = declarative_base()


class Artists(DBase):
    """ DB of artists """
    __tablename__ = "artists"

    id = Column(Integer, primary_key=True)
    element = Column('element', String)
    mbid = Column('mbid', String, nullable=True)
    playcount = Column('playcount', Integer)
    listcount = Column('listcount', Integer)
    username = Column('username', String)


class ArtistTags(DBase):
    """ DB of artist tags """
    __tablename__ = "artisttags"

    id = Column(Integer, primary_key=True)
    tag = Column('tag', String)
    weight = Column('weight', Integer, nullable=True)
    artist_id = Column('artist_id', Integer, ForeignKey('artists.id'), nullable=False)


class Tracks(DBase):
    """ DB of tracks """
    __tablename__ = "tracks"

    id = Column(Integer, primary_key=True)
    element = Column('element', String)
    album = Column('album', String, nullable=True)
    song = Column('song', String, nullable=True)
    timestamp = Column('timestamp', Integer)
    username = Column('username', String)


def create_db():
    engine = create_engine("postgresql://gonzalorivero:root@localhost:5432/lastfm")
    DBase.metadata.create_all(engine)


if __name__ == "__main__":
    create_db()
