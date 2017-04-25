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
    max_date = getMaxDate()
    min_date = getMinDate()
    getScrobbles(max_date, min_date)
    #insertScrobbles()

    if _errors.error is '':
        _errors.error = 'No errors, successfully updated scrobbles.'

    print 'Result: ' + _errors.error

"""
    Gets the
"""
def getScrobbles(max_date, min_date):
    global lst
    keep_going = 1
    page = 0
    lst = list()
    while keep_going > 0:
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
                    u = int(_track['date']['uts'])
                    dtTrack = DTime.datetime.utcfromtimestamp(u)
                    if dtTrack > max_date:
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
                    elif dtTrack < min_date:
                        keep_going = 1
                        s = scrobbles(_track['date']['uts'], _track['date']['#text'], _track['name'], _track['mbid'], _track['artist']['#text'], _track['artist']['mbid'], _track['album']['#text'], _track['album']['mbid'], -1)
                        lst.append(s)
                    elif dtTrack > min_date:
                        keep_going = 1
                        break
                    else:
                        #lst.reverse()
                        #return
                        break
            #keep_going = 0
            #insertScrobbles()
        except Exception as e:
            _errors.lst_errors.append(str(e))
    insertScrobbles()

"""Gets the last max scrobble date from db"""
def getMaxDate():
    cnxn = psycopg2.connect(dbname=vars.sql_db, user=vars.sql_user, password=vars.sql_pass, host=vars.sql_srv)
    cursor = cnxn.cursor()
    date = 0
    try:
        cursor.execute('select max(scrobble_time_utc) from _scrobbles;')
        row = cursor.fetchone()
        date = row[0]
    except Exception as e:
        print 'missing latest date'

    cursor.close()
    cnxn.close()
    return date


def getMinDate():
    cnxn = psycopg2.connect(dbname=vars.sql_db, user=vars.sql_user, password=vars.sql_pass, host=vars.sql_srv)
    cursor = cnxn.cursor()
    date = 0
    try:
        cursor.execute('select min(scrobble_time_utc) from _scrobbles;')
        row = cursor.fetchone()
        date = row[0]
    except Exception as e:
        print 'missing latest date'

    cursor.close()
    cnxn.close()
    return date

def insertScrobble():
    global lst
    cnxn = psycopg2.connect(dbname=vars.sql_db, user=vars.sql_user, password=vars.sql_pass, host=vars.sql_srv)
    cursor = cnxn.cursor()
    try:
        for s in lst:
            try:
                #cursor.execute('insert into public._scrobbles (scrobble_time_utc, title, track_mbid, artist, artist_mbid, album, album_mbid) values (%s, %s, %s, %s, %s, %s, %s);', (s.iso_time, s.track_name, s.track_mbid, s.artist_name, s.artist_mbid, s.album_name, s.album_mbid))
                cursor.execute('insert into public._scrobbles (scrobble_time_utc, title, track_mbid, artist, artist_mbid, album, album_mbid) select %s, %s, %s, %s, %s, %s, %s where not exists (select * from _scrobbles where scrobble_time_utc = %s and title = %s);',(s.iso_time, s.track_name, s.track_mbid, s.artist_name, s.artist_mbid, s.album_name, s.album_mbid, s.iso_time, s.track_name))
            except Exception as x:
                str(x)
        cnxn.commit()
        cursor.close()
        cnxn.close()
    except Exception as e:
        _errors.lst_errors.append(str(e))

def insertScrobbles():
    global lst
    if len(lst) > 0:
        try:
            # for s in lst:
            #     insertScrobble(s)
            insertScrobble()
        except Exception as e:
            _errors.lst_errors.append(str(e))
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





