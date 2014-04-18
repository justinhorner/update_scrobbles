__author__ = 'justinhorner'

import pyodbc
import ConfigParser

"""
- Method to get scrobbles from lastfm api
- Method to loop through pages until last date found
- Method to insert scrobbles into db
"""

last_fm_api_key = ''
last_fm_user = ''
sql_pass = ''
sql_user = ''

def xecute():
    load_config()
    _date = getMaxDate()
    getScrobbles("")

"""
    Gets the
"""
def getScrobbles(page):
    api_url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=%s&api_key=%s&format=json&page=%s' % (last_fm_user, last_fm_api_key, page)


"""Gets the last max scrobble date from db"""
def getMaxDate():
    print 
    cnxn = pyodbc.connect('DRIVER={PostgreSQL Unicode};SERVER=justinhorner.me;DATABASE=mypgdb;UID=' + sql_user + ';PWD=' + sql_pass)
    cursor = cnxn.cursor()
    rows = cursor.execute('select max(unixtime) from public.lastfm_scrobbles;').fetchone()
    date = rows
    print rows

    return date

def load_config():
    configParser = ConfigParser.RawConfigParser()
    configFilePath = r'config'
    configParser.read(configFilePath)

    last_fm_api_key = configParser.get('configs', 'last_fm_api_key')
    last_fm_user = configParser.get('configs', 'last_fm_user')
    sql_user = configParser.get('configs', 'sql_user')
    sql_pass = configParser.get('configs', 'sql_pass')



xecute()




