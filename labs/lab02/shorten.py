#
# shorten.py
#
# Implements API for a URL shortening service
#
# Original author:
#   Prof. Joe Hummel
#   Northwestern University
# Alec do Couto

import pymysql
from configparser import ConfigParser


###################################################################
#
# get_dbConn
#
# create and return connection object, based on configuration
# information in shorten-config.ini
#
def get_dbConn():
  """
  Reads the configuration info from shorten-config.ini, creates
  a pymysql connection object based on this info, and returns it

  Parameters
  ----------
  N/A

  Returns
  -------
  pymysql connection object
  """

  try:
    #
    # obtain database server config info:
    #
    config_file = 'shorten-config.ini'    
    configur = ConfigParser()
    configur.read(config_file)

    endpoint = configur.get('rds', 'endpoint')
    portnum = int(configur.get('rds', 'port_number'))
    username = configur.get('rds', 'user_name')
    pwd = configur.get('rds', 'user_pwd')
    dbname = configur.get('rds', 'db_name')

    #
    # now create connection object and return it:
    #
    dbConn = pymysql.connect(host=endpoint,
                            port=portnum,
                            user=username,
                            passwd=pwd,
                            database=dbname)

    return dbConn
  
  except Exception as err:
    print("**ERROR in shorten.get_dbConn():")
    print(str(err))
    return None
  

###################################################################
#
# get_url
#
# Looks up the short url in the database, returning the associated
# long url. Each time this is done, the count for that url is 
# incremented.
#
def get_url(shorturl):
  """
  Looks up the short url in the database, returning the associated
  long url. Each time this is done, the count for that url is 
  incremented.

  Parameters
  ----------
  shorturl : the short URL to lookup (string)

  Returns
  -------
  long URL (string), or empty string if short URL not found
  """

  try:
    dbConn = get_dbConn()

    longurl = ""
    select_sql = "SELECT LongUrl FROM LinksTable WHERE ShortUrl = %s"
    update_sql = "UPDATE LinksTable SET LookedUpCount = LookedUpCount + 1 WHERE ShortUrl = %s"
    
    dbConn.begin()
    dbCursor = dbConn.cursor()

    dbCursor.execute(update_sql, (shorturl,))

    dbCursor.execute(select_sql, (shorturl,))
    row = dbCursor.fetchone()
    if row:
      longurl = row[0]
    
    dbConn.commit()
    return longurl

  except Exception as err:
    print("**ERROR in shorten.get_url():")
    print(str(err))
    dbConn.rollback()
    return ""

  finally:
    if dbConn is not None: dbConn.close()


##################################################################
#
# get_stats
#
# Returns the count for the given short url, which represents
# the # of times the short url has been looked up
#
def get_stats(shorturl):
  """
  Looks up the short url and returns the count

  Parameters
  __________
  shorturl : the short URL to lookup (string)

  Returns
  _______
  the count associated with the short url, -1 if short URL not found
  """

  try:
    dbConn = get_dbConn()
    select_sql = "SELECT LookedUpCount FROM LinksTable WHERE ShortUrl = %s"

    dbCursor = dbConn.cursor()

    dbCursor.execute(select_sql, (shorturl,))
    row = dbCursor.fetchone()
    if row:
      return row[0]
    else:
      return -1

  except Exception as err:
    print("**ERROR in shorten.get_stats():")
    print(str(err))
    return -1

  finally:
    if dbConn is not None: dbConn.close()


##################################################################
#
# put_shorturl:
#
# Maps the long url to the short url by inserting both urls
# into the database, returning True if successful. If the
# short url already exists in the database (and is mapped to
# a different long url), the database is left unchanged and
# False is returned since the short url is already taken.
#
def put_shorturl(longurl, shorturl):
  """
  Maps the long url to the short url by inserting both urls
  into the database with a count of 0. Fails if the short
  url is already taken AND mapped to a different long url.

  Parameters
  __________
  the original long URL (string)
  the desired short URL (string)

  Returns
  _______
  True if successful, False if not
  """

  try:
    dbConn = get_dbConn()

    dbConn.begin()
    dbCursor = dbConn.cursor()

    check_sql = "SELECT LongUrl FROM LinksTable WHERE ShortUrl = %s"
    dbCursor.execute(check_sql, (shorturl,))
    row = dbCursor.fetchone()
    if row:
      if row[0] == longurl:
        dbConn.commit()
        return True
      else:
        dbConn.commit()
        return False
      
    insert_sql = "INSERT INTO LinksTable (ShortUrl, LongUrl, LookedUpCount) VALUES(%s, %s, 0)"
    dbCursor.execute(insert_sql, (shorturl, longurl))
    dbConn.commit()
    
    return True

  except Exception as err:
    print("**ERROR in shorten.put_shorturl():")
    print(str(err))
    dbConn.rollback()
    return False

  finally:
    if dbConn is not None: dbConn.close()


###############################################################
#
# put_reset
#
# Deletes all the urls from the database
#
def put_reset():
  """
  Deletes all the urls from the database

  Parameters
  __________
  N/A

  Returns
  _______
  True if successful, False if not
  """

  try:
    dbConn = get_dbConn()
    dbConn.begin()
    
    dbCursor = dbConn.cursor()
    delete_sql = "DELETE FROM LinksTable"

    dbCursor.execute(delete_sql)
    dbConn.commit()
    return True
  
  except Exception as err:
    print("**ERROR in shorten.put_reset():")
    print(str(err))
    dbConn.rollback()
    return False

  finally:
    if dbConn is not None: dbConn.close()
