__author__ = 'justinhorner'

import psycopg2
import ConfigParser
import datetime as DTime
import requests

"""
- Method to get scrobbles from lastfm api
- Method to loop through pages until last date found
- Method to insert scrobbles into db
"""

lst = list()

class _errors:
    lst_errors = list()
    def __init__(self, _error):
        self.error = _error

class vars:
    last_fm_api_key = ''
    last_fm_user = ''
    sql_pass = ''
    sql_user = ''
    sql_srv = ''
    sql_db = ''
    sql_drv = ''

class scrobbles:
    def __init__(self, unixtime, iso_time, track_name, track_mbid, artist_name, artist_mbid, album_name, album_mbid, _track_id):
        self.unixtime = unixtime
        self.iso_time = iso_time
        self.track_name = track_name
        self.track_mbid = track_mbid
        self.artist_name = artist_name
        self.artist_mbid = artist_mbid
        self.album_name = album_name
        self.album_mbid = album_mbid
        self._track_id = _track_id


def xecute():
    load_config()
    _date = getMaxDate()
    getScrobbles(_date)
    #insertScrobbles()

    if _errors.error is '':
        _errors.error = 'No errors, successfully updated scrobbles.'

    print 'Result: ' + _errors.error

"""
    Gets the
"""
def getScrobbles(_date):
    global lst
    keep_going = 1
    page = 0
    while keep_going > 0:
        lst = list()
        keep_going = 0
        page = page + 1
        api_url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=%s&api_key=%s&format=json&page=%s' % (vars.last_fm_user, vars.last_fm_api_key, str(page))
        try:
            response = requests.get(api_url)
            hdr = response.headers
            content = response.content
            jsn = response.json()
            recentTracks = jsn['recenttracks']

            for _track in recentTracks['track']:
                if '@attr' in _track:
                    continue
                else:
                    i = int(_track['date']['uts'])
                    if i > _date:
                        keep_going = 1
                        """
                        s.unixtime = _track['date']['uts']
                        s.iso_time = _track['date']['#text']
                        s.track_name = _track['name']
                        s.track_mbid = _track['mbid']
                        s.artist_name = _track['artist']['#text']
                        s.artist_mbid = _track['artist']['mbid']
                        s.album_name = _track['album']['#text']
                        s.album_mbid = _track['album']['mbid']
                        """
                        s = scrobbles(_track['date']['uts'], _track['date']['#text'], _track['name'], _track['mbid'], _track['artist']['#text'], _track['artist']['mbid'], _track['album']['#text'], _track['album']['mbid'], -1)
                        #lst.append(s)
                        lst.append(s)
                    else:
                        #lst.reverse()
                        return
            #keep_going = 0
            insertScrobbles()
        except Exception as e:
            _errors.error += str(e)

"""Gets the last max scrobble date from db"""
def getMaxDate():
    cnxn = psycopg2.connect(dbname=vars.sql_db, user=vars.sql_user, password=vars.sql_pass, host=vars.sql_srv)
    cursor = cnxn.cursor()
    date = 0
    try:
        rows = cursor.execute('select max(scrobble_time) from public.scrobbles;').fetchone()
        date = int(rows[0])
    except:
        print 'missing latest date'

    cursor.close()
    cnxn.close()
    return date

def insertNonScrobble(_type, name, oid, mbid):
    cnxn = psycopg2.connect(dbname=vars.sql_db, user=vars.sql_user, password=vars.sql_pass, host=vars.sql_srv)
    cursor = cnxn.cursor()

    iReturn = -1

    try:
        if _type == 'artist':
            cursor.execute('insert into public.scrobbles_artists (name, artist_mbid) select %s, %s where not exists(select * from public.scrobbles_artists where artist_mbid = %s); SELECT max(artist_id) from public.scrobbles_artists;', (name, mbid, mbid))
        elif _type == 'album':
            cursor.execute('insert into public.scrobbles_albums (name, artist_id, album_mbid) select %s, %s, %s where not exists(select * from public.scrobbles_albums where album_mbid = %s limit 1); SELECT max(album_id) from public.scrobbles_albums;', (name, oid, mbid, mbid))
        elif _type == 'track':
            cursor.execute('insert into public.scrobbles_tracks (name, album_id, track_mbid) select %s, %s, %s where not exists(select * from public.scrobbles_tracks where track_mbid = %s limit 1); SELECT max(track_id) from public.scrobbles_tracks', (name, oid, mbid, mbid))

        cnxn.commit()
        row = cursor.fetchone()
        iReturn = row[0]
        cursor.close()
        cnxn.close()

    except Exception as e:
        _errors.error += str(e)

    return iReturn

def insertScrobble(s):
    cnxn = psycopg2.connect(dbname=vars.sql_db, user=vars.sql_user, password=vars.sql_pass, host=vars.sql_srv)
    cursor = cnxn.cursor()
    try:
        cursor.execute('insert into public.scrobbles (scrobble_time, title, track_id) values (%s, %s, %s)', (s.iso_time, s.track_name, s._track_id))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    except Exception as e:
        _errors.error += str(e)

def insertScrobbles():
    global lst
    if len(lst) > 0:
        try:
            for s in lst:
                artist_id = insertNonScrobble('artist', s.artist_name, -1, s.artist_mbid)
                album_id = insertNonScrobble('album', s.album_name, artist_id, s.album_mbid)
                s._track_id = insertNonScrobble('track', s.track_name, album_id, s.track_mbid)
                insertScrobble(s)
        except Exception as e:
            _errors.error += str(e)
    else:
        _errors.error = 'No new scrobbles, nothing updated'

def load_config():
    configParser = ConfigParser.RawConfigParser()
    configFilePath = r'config'
    configParser.read(configFilePath)

    vars.last_fm_api_key = configParser.get('configs', 'last_fm_api_key')
    vars.last_fm_user = configParser.get('configs', 'last_fm_user')
    vars.sql_user = configParser.get('configs', 'sql_user')
    vars.sql_pass = configParser.get('configs', 'sql_pass')
    vars.sql_srv = configParser.get('configs', 'sql_srv')
    vars.sql_db = configParser.get('configs', 'sql_db')
    vars.sql_drv = configParser.get('configs', 'sql_driver_win')

xecute()





