__author__ = 'justinhorner'

import pyodbc
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
    def __init__(self, unixtime, iso_time, track_name, track_mbid, artist_name, artist_mbid, album_name, album_mbid):
        self.unixtime = unixtime
        self.iso_time = iso_time
        self.track_name = track_name
        self.track_mbid = track_mbid
        self.artist_name = artist_name
        self.artist_mbid = artist_mbid
        self.album_name = album_name
        self.album_mbid = album_mbid


def xecute():
    load_config()
    _date = getMaxDate()
    getScrobbles(_date)
    insertScrobbles()

    if _errors.error is '':
        _errors.error = 'No errors, successfully updated scrobbles.'

    print 'Result: ' + _errors.error

"""
    Gets the
"""
def getScrobbles(_date):
    keep_going = 1
    page = 0
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
                        s = scrobbles(_track['date']['uts'], _track['date']['#text'], _track['name'], _track['mbid'], _track['artist']['#text'], _track['artist']['mbid'], _track['album']['#text'], _track['album']['mbid'])
                        #lst.append(s)
                        lst.append(s)
                    else:
                        lst.reverse()
                        return
        except Exception as e:
            _errors.error += str(e)



"""Gets the last max scrobble date from db"""
def getMaxDate():
    cnxn = pyodbc.connect('DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;SSLMODE=require' % (vars.sql_drv, vars.sql_srv, vars.sql_db, vars.sql_user, vars.sql_pass))
    cursor = cnxn.cursor()
    rows = cursor.execute('select max(unixtime) from public.lastfm_scrobbles;').fetchone()
    date = int(rows[0])
    return date

def insertScrobbles():
    if len(lst) > 0:
        try:
            cnxn = pyodbc.connect('DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;SSLMODE=require' % (vars.sql_drv, vars.sql_srv, vars.sql_db, vars.sql_user, vars.sql_pass))
            cursor = cnxn.cursor()
            for l in lst:
                cursor.execute('insert into public.lastfm_scrobbles (iso_time, unixtime, track_name, track_mbid, artist_name, artist_mbid, album_name, album_mbid) values (?, ?, ?, ?, ?, ?, ?, ?)', l.iso_time, l.unixtime, l.track_name, l.track_mbid, l.artist_name, l.artist_mbid, l.album_name, l.album_mbid)
                cnxn.commit()
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





