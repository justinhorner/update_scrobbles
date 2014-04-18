__author__ = 'justinhorner'

import pyodbc
import ConfigParser

"""
- Method to get scrobbles from lastfm api
- Method to loop through pages until last date found
- Method to insert scrobbles into db
"""

class vars:
    last_fm_api_key = ''
    last_fm_user = ''
    sql_pass = ''
    sql_user = ''
    sql_srv = ''
    sql_db = ''


def xecute():
    load_config()
    _date = getMaxDate()
    getScrobbles(_date)

"""
    Gets the
"""
def getScrobbles(_date):
    page = '1'
    api_url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=%s&api_key=%s&format=json&page=%s' % (vars.last_fm_user, vars.last_fm_api_key, vars.page)


"""Gets the last max scrobble date from db"""
def getMaxDate():
    print 
    cnxn = pyodbc.connect('DRIVER={PostgreSQL Unicode};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;SSLMODE=require' % (vars.sql_srv, vars.sql_db, vars.sql_user, vars.sql_pass))
    cursor = cnxn.cursor()
    rows = cursor.execute('select max(unixtime) from public.lastfm_scrobbles;').fetchone()
    date = row
    print rows

    return date

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




xecute()




