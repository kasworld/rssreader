#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""write program discription
made by kasw
copyright 2013
Version""" 

Version = '1.0.0'

import os,io,sys,os.path,math,random,re,datetime,time,string,pprint,uuid,urllib
import optparse,struct,multiprocessing,binascii,itertools,functools,logging,exceptions

import sys,os.path
srcdir = os.path.dirname(os.path.abspath(sys.argv[0]))
sys.path.extend( [
    os.path.join( srcdir , 'kaswlib' ),
    os.path.join( os.path.split( srcdir)[0] , 'kaswlib' ),
    ])
try :
    from kaswlib import *
except :
    print 'kaswlib import fail'
    pass

logger = getLogger(logging.DEBUG,'kaswrssreader')

import signal
def sigstophandler(signum, frame):
    print 'User Termination'
#signal.signal(signal.SIGINT, sigstophandler)


import feedparser
from bs4 import BeautifulSoup
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.ext.declarative

engine = sqlalchemy.create_engine("sqlite:///feeddb.sqlite" , convert_unicode=True)
db_session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=engine))
Model = sqlalchemy.ext.declarative.declarative_base()
Model.query = db_session.query_property()

class Feed(Model):
    __tablename__ = "feed"
    id = sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
    feed_url = sqlalchemy.Column(sqlalchemy.String(1024))
    lastChecked = sqlalchemy.Column(sqlalchemy.DateTime)
    title = sqlalchemy.Column(sqlalchemy.String(128))

    def __init__(self, feed_url=None, lastChecked=None, title=u"Some feed"):
        self.feed_url = feed_url
        self.lastChecked = lastChecked if lastChecked else datetime.datetime.now()
        self.title = title

class Entry(Model):
    __tablename__ = "entry"
    id = sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
    feed_id = sqlalchemy.Column(sqlalchemy.Integer)
    published = sqlalchemy.Column(sqlalchemy.DateTime)
    updated = sqlalchemy.Column(sqlalchemy.DateTime)
    title = sqlalchemy.Column(sqlalchemy.String(1024))
    content = sqlalchemy.Column(sqlalchemy.Text)
    description = sqlalchemy.Column(sqlalchemy.String(256))
    link = sqlalchemy.Column(sqlalchemy.String(1024))
    remote_id = sqlalchemy.Column(sqlalchemy.String(1024))

    def __init__(self, feed_id=None, published=None, updated=None, title=None, content=None, description=None,
                 link=None, remote_id=None,):
        self.feed_id = feed_id
        self.published = published
        self.updated = updated
        self.title = title
        self.content = content
        self.description = description
        self.link = link
        self.remote_id = remote_id

Model.metadata.create_all(bind=engine)

def mkDateTimeFromstruct_time( src ):
    if not src :
        return datetime.datetime.now()
    return datetime.datetime( *src[:6] )

from xml.sax import SAXException
def feed_requester(feed_url,resultqueue,errqueue):
    logger = multiprocessing.log_to_stderr()
    logger.setLevel(logging.INFO)
    try:
        d = feedparser.parse(feed_url)
    except SAXException as errno:
        logger.debug("Failed to retrive {0}\nTraceback:\n{1}".format(feed_url, errno))
        errqueue.put(feed_url)
        return 

    if not d:
        logger.debug("Failed to retrive {0}\n".format(feed_url))
        errqueue.put(feed_url)
        return 
    if d.bozo:
        logger.warning("retrive not successful {0}, Bozo error. {1}\n".format(feed_url, d.bozo_exception) )
        if isinstance( d.bozo_exception , feedparser.CharacterEncodingOverride ):
            pass
        else :
            errqueue.put(feed_url)
            return 

    for entry in d.entries:
        try:
            description = entry.get("description", "") 
            if description == "" :
                description = entry.get('summary')
            body = entry.get("content", [{}] )[0].get("value", description ) 
            post = {
                "title": entry.get("title", "No title"),
                "link": entry.get("link", "#"),
                "id": entry.get("id", "No Id"),
                "published":  mkDateTimeFromstruct_time( entry.get("published_parsed")  ),
                "updated": mkDateTimeFromstruct_time( entry.get("updated_parsed") ),
                "description": description,
                "content":  body
            }
            resultqueue.put((feed_url,post))
        except exceptions.Exception as errno:
            logger.error(errno)
            continue
    return 

def add_entry(entry, feed_id):
    stored_ent = db_session.query(Entry).filter_by(link=entry.get("link")).first()
    #Even if it is in db it might be updated since last time.
    if stored_ent is not None and stored_ent.updated == entry.get("updated"):
        return
    if stored_ent:
        db_session.query(Entry).filter_by(id=stored_ent.id).update(
            {
                "published": entry.get("published"),
                "updated": entry.get("updated"),
                "title": entry.get("title"),
                "content": entry.get("content"),
                "description": entry.get("description"),
                "link": entry.get("link"),
                "remote_id": entry.get("id"),
                "feed_id": feed_id,
                })
        logger.debug("Updating entry with id: {0}".format(entry.get("id")))
    else:
        new_entry = Entry(
            published=entry.get("published"),
            updated=entry.get("updated"),
            title=entry.get("title"),
            content=entry.get("content"),
            description=entry.get("description"),
            link=entry.get("link"),
            remote_id=entry.get("id"),
            feed_id=feed_id
            )
        logger.debug(u"Adding new entry with id: {0}".format(entry.get("id")))
        db_session.add(new_entry)
    db_session.commit()

def importOPMLFile(opmlfilename=None):
    if opmlfilename :
        try :
            opml = BeautifulSoup(open(opmlfilename))
        except :
            print 'fail to load opml', opmlfilename
            opml = None
    if opml :
        for feed in  opml('outline') :
            stored_feed = db_session.query(Feed).filter_by(feed_url=feed['xmlurl']).first()
            if not stored_feed :
                # if not , insert new Feed
                f = Feed(
                    feed_url=feed['xmlurl'], 
                    lastChecked = datetime.datetime.now() - datetime.timedelta(1), 
                    title=feed['title'])
                db_session.add(f)
            db_session.commit()
        os.rename( opmlfilename, opmlfilename + '.imported')

def updateAll(opmlfilename=None):
    importOPMLFile(opmlfilename)

    # loop all feed , and upate
    feeds = db_session.query(Feed)

    mqresult = multiprocessing.Queue()
    mqerrresult = multiprocessing.Queue()

    processlist = []

    for feed in feeds:
        # skip if less an hour
        if datetime.datetime.now() - feed.lastChecked <  datetime.timedelta(hours=1) :
            continue
        cp = multiprocessing.Process(
                target = feed_requester,
                args = (feed.feed_url , mqresult ,mqerrresult),
                name = feed.feed_url
                )
        processlist.append( cp )
        cp.start()

    st =  time.time()
    while True :
        if time.time() - st > 1 :
            allivecount = 0
            for t in processlist :
                if t.exitcode == None :
                    allivecount += 1
            if allivecount == 0:
                break
            st = time.time()

        if not mqresult.empty() :
            feed_url, entry  = mqresult.get()
            feed = db_session.query(Feed).filter_by(feed_url = feed_url).first()        
            add_entry(entry, feed.id)
            feed.lastChecked = datetime.datetime.now() # update lastchecked

        if not mqerrresult.empty():
            feed_url = mqerrresult.get()
            feed = db_session.query(Feed).filter_by(feed_url = feed_url).first()
            if feed :
                db_session.delete(feed)
                db_session.commit()

    for t in processlist :
        t.join()

if __name__ == "__main__":
    updateAll('subscriptions.xml')

# vim:ts=4:sw=4:et