#
# PhotoApp API functions that interact with PhotoApp web service
# to support downloading and uploading images to S3, along with
# retrieving and updating data in associated photoapp database.
#
# Initial code (initialize, get_ping, get_users):
#   Prof. Joe Hummel
#   Northwestern University
#

import base64
import logging
import os
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from configparser import ConfigParser


#
# module-level varibles:
#
WEB_SERVICE_URL = 'set via call to initialize()'


def _validate_local_filename(local_filename):
  if local_filename is None:
    raise ValueError("local_filename is required")

  if local_filename.strip() == "":
    raise ValueError("local_filename cannot be empty")

  if not os.path.isfile(local_filename):
    raise ValueError("local_filename does not exist or is not a file")

  return local_filename


def _build_url(path):
  base = WEB_SERVICE_URL.rstrip('/')
  return f"{base}{path}"


def _safe_json(response):
  try:
    return response.json()
  except ValueError:
    msg = f"status code {response.status_code}: invalid JSON response"
    raise HTTPError(msg)

###################################################################
#
# initialize
#
# Initializes local environment need to access PhotoApp web 
# service, based on given client-side configuration file. Call
# this function only once, and call before calling any other 
# API functions.
#
# NOTE: does not check to make sure we can actually reach the
# web service. Call get_ping() to check.
#
def initialize(client_config_file):
  """
  Initializes local environment for AWS access, returning True
  if successful and raising an exception if not. Call this 
  function only once, and call before calling any other API
  functions.
  
  Parameters
  ----------
  client_config_file is the name of the client-side configuration 
  file, probably 'photoapp-client-config.ini', which contains URL 
  for web service.
  
  Returns
  -------
  True if successful, raises an exception if not
  """

  try:
    #
    # extract and save URL of web service for other API functions:
    #
    global WEB_SERVICE_URL

    configur = ConfigParser()
    configur.read(client_config_file)
    WEB_SERVICE_URL = configur.get('client', 'webservice')

    #
    # success:
    #
    return True

  except Exception as err:
    logging.error("initialize():")
    logging.error(str(err))
    raise


###################################################################
#
# get_ping
#
# To "ping" a system is to see if it's up and running. This 
# function pings the bucket and the database server to make
# sure they are up and running. Returns a tuple (M, N), where
#
#   M = # of items in the photoapp bucket
#   N = # of users in the photoapp.users table
#
# If an error occurs / a service is not accessible, M or N
# will be an error message. Hopefully the error messages will
# convey what is going on (e.g. no internet connection).
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def get_ping():
  """
  Based on the configuration file, retrieves the # of items in the S3 bucket and
  the # of users in the photoapp.users table. Both values are returned as a tuple
  (M, N). If an error occurs, e.g. S3 or the database is unreachable, then an 
  exception is raised.
  
  Parameters
  ----------
  N/A
  
  Returns
  -------
  the tuple (M, N) where M is the # of items in the S3 bucket and
  N is the # of users in the photoapp.users table. If an error 
  occurs, e.g. S3 or the database is unreachable, then an exception
  is raised.
  """

  try:
    url = _build_url("/ping")

    response = requests.get(url)
    body = _safe_json(response)

    if response.status_code == 200:
      #
      # success
      #
      M = body['M']
      N = body['N']
      return (M, N)
    else:
      #
      # failed:
      #
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      #
      # NOTE: this exception will not trigger retry mechanism, 
      # since we reached the server and the server-side failed, 
      # and we are assuming the server-side is also doing retries.
      #
      raise HTTPError(err_msg)

  except Exception as err:
    logging.error("get_ping():")
    logging.error(str(err))
    #
    # raise exception to trigger retry mechanism if appropriate:
    #
    raise

  finally:
    # nothing to do
    pass


###################################################################
#
# get_users
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def get_users():
  """
  Returns a list of all the users in the database. Each element 
  of the list is a tuple containing userid, username, givenname
  and familyname (in this order). The tuples are ordered by 
  userid, ascending. If an error occurs, an exception is raised.
  
  Parameters
  ----------
  N/A
  
  Returns
  -------
  a list of all the users, where each element of the list is a tuple
  containing userid, username, givenname, and familyname in that 
  order. The list is ordered by userid, ascending. On error an 
  exception is raised.
  """

  try:
    url = _build_url("/users")

    response = requests.get(url)
    body = _safe_json(response)

    if response.status_code == 200:
      #
      # success
      #
      rows = body['data']

      # 
      # rows is a dictionary-like list of objects, so
      # let's extract the values and discard the keys
      # to honor the API's return value:
      #
      users = []

      for row in rows:
        userid = row["userid"]
        username = row["username"]
        givenname = row["givenname"]
        familyname = row["familyname"]
        #
        user = (userid, username, givenname, familyname)
        users.append(user)

      return users
    else:
      #
      # failed:
      #
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      #
      # NOTE: this exception will not trigger retry mechanism, 
      # since we reached the server and the server-side failed, 
      # and we are assuming the server-side is also doing retries.
      #
      raise HTTPError(err_msg)

  except Exception as err:
    logging.error("get_users():")
    logging.error(str(err))
    #
    # raise exception to trigger retry mechanism if appropriate:
    #
    raise

  finally:
    # nothing to do
    pass


###################################################################
#
# get_images
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def get_images(userid = None):
  """
  Returns a list of all the images in the database. Each element 
  of the list is a tuple containing assetid, userid, localname
  and bucketkey (in this order). The list is ordered by assetid, 
  ascending. If a userid is given, then just the images with that 
  userid are returned; validity of the userid is not checked, 
  which implies that an empty list is returned if the userid is 
  invalid. If an error occurs, an exception is raised.
  
  Parameters
  ----------
  userid (optional) filters the returned images for just this userid
  
  Returns
  -------
  a list of images, where each element of the list is a tuple
  containing assetid, userid, localname, and bucketkey in that order.
  The list is ordered by assetid, ascending. If an error occurs, 
  an exception is raised.
  """

  try:
    url = _build_url("/images")

    params = {}
    if userid is not None:
      params['userid'] = userid

    response = requests.get(url, params=params)
    body = _safe_json(response)

    if response.status_code == 200:
      #
      # success
      #
      rows = body['data']


      images = []

      for row in rows:
        assetid = row["assetid"]
        userid = row["userid"]
        localname = row["localname"]
        bucketkey = row["bucketkey"]

        image = (assetid, userid, localname, bucketkey)
        images.append(image)

      return images
    else:
      #
      # failed:
      #
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      #
      # NOTE: this exception will not trigger retry mechanism, 
      # since we reached the server and the server-side failed, 
      # and we are assuming the server-side is also doing retries.
      #
      raise HTTPError(err_msg)
  except Exception as err:
    logging.error("get_images():")
    logging.error(str(err))
    #
    # raise exception to trigger retry mechanism if appropriate:
    #
    raise
  finally:
    # nothing to do
    pass    

###################################################################
#
# post_image
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def post_image(userid, local_filename):
  """
  Uploads an image to S3 with a unique name, allowing the same local
  file to be uploaded multiple times if desired. A record of this 
  image is inserted into the database, and upon success a unique
  assetid is returned to identify this image. The image is also 
  analyzed by the Rekognition AI service to label objects within
  the image; the results of this analysis are also saved in the
  database (and can be retrieved later via get_image_labels). If 
  an error occurs, an exception is raised. An invalid userid is 
  considered a ValueError, "no such userid".

  Parameters
  ----------
  userid for whom we are uploading this image
  local filename of image to upload
  
  Returns
  -------
  image's assetid upon success, raises an exception on error
  """
  try :
    url = _build_url(f"/image/{userid}")

    filename = _validate_local_filename(local_filename)

    infile = open(filename, 'rb')
    image_bytes = infile.read()
    infile.close()

    encoded_bytes = base64.b64encode(image_bytes)
    image_str = encoded_bytes.decode()
    data = {
      'local_filename': os.path.basename(filename),
      "data": image_str
    }
# params CREATES A QUERY PARAMETER, but we want a url parameterer. Stupid
    response = requests.post(url, json=data)
    status_code = response.status_code
    body = _safe_json(response)
    if status_code == 200:
      return body['assetid']
    else:
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      #
      # NOTE: this exception will not trigger retry mechanism, 
      # since we reached the server and the server-side failed, 
      # and we are assuming the server-side is also doing retries.
      #
      raise HTTPError(err_msg)
  
  except Exception as err:
    logging.error("post_image():")
    logging.error(str(err))
    raise
  finally:
    # nothing to do
    pass    

###################################################################
#
# get_image
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def get_image(assetid, local_filename = None):
  """
  Downloads the image from S3 denoted by the provided asset. If a
  local_filename is provided, the newly-downloaded file is saved
  with this filename (overwriting any existing file with this name).
  If a local_filename is not provided, the newly-downloaded file
  is saved using the local filename that was saved in the database
  when the file was uploaded. If successful, the filename for the
  newly-downloaded file is returned; if an error occurs then an
  exception is raised. An invalid assetid is considered a
  ValueError, "no such assetid".
  
  Parameters
  ----------
  assetid of image to download
  local filename (optional) for newly-downloaded image
  
  Returns
  -------
  local filename for the newly-downloaded file, or raises an 
  exception upon error
  """
  
  try:
    url = _build_url(f"/image/{assetid}")
    response = requests.get(url)
    
    if response.status_code == 200:
      body = _safe_json(response)
      #
      # success
      #
      original_filename = body['local_filename']
      image_base64 = body['data']
      
      image_bytes = base64.b64decode(image_base64)
      
      filename_to_save = local_filename if local_filename is not None else original_filename
      
      with open(filename_to_save, 'wb') as outfile:
        outfile.write(image_bytes)
      
      return filename_to_save
    
    elif response.status_code in [400, 500]:
      body = _safe_json(response)
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      #
      # NOTE: this exception will not trigger retry mechanism, 
      # since we reached the server and the server-side failed, 
      # and we are assuming the server-side is also doing retries.
      #
      raise ValueError(err_msg)
    
    else:
      #
      # server-side error (404, etc.)
      #
      response.raise_for_status()
  
  except Exception as err:
    logging.error("get_image():")
    logging.error(str(err))

    raise
  
  finally:
    pass

###################################################################
#
# get_image_labels
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def get_image_labels(assetid):
  """
  When an image is uploaded to S3, the Rekognition AI service is
  automatically called to label objects in the image. Given the 
  image assetid, this function retrieves those labels. In
  particular this function returns a list of tuples. Each tuple
  is of the form (label, confidence), where label is a string 
  (e.g. 'sailboat') and confidence is an integer (e.g. 90).
  The tuples are ordered by label, ascending. If an error occurs
  an exception is raised; an invalid assetid is considered a
  ValueError, "no such assetid".

  Parameters
  ----------
  image assetid to retrieve labels for

  Returns
  -------
  a list of labels identified in the image, where each element
  of the list is a tuple of the form (label, confidence) where
  label is a string and confidence is an integer. If an error
  occurs an exception is raised; an invalid assetid is considered
  a ValueError, "no such assetid".
  """

  try:
    url = _build_url("/image_labels")

    params = {'assetid': assetid}

    response = requests.get(url, params=params)

    if response.status_code == 200:
      body = _safe_json(response)
      #
      # success
      #
      rows = body['data']

      labels = []

      for row in rows:
        label = row["label"]
        confidence = row["confidence"]
        labels.append((label, confidence))

      return labels

    elif response.status_code == 400:
      body = _safe_json(response)
      msg = body['message']
      raise ValueError(msg)

    elif response.status_code == 500:
      body = _safe_json(response)
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      raise HTTPError(err_msg)

    else:
      #
      # Any other error (404, etc.)
      #
      response.raise_for_status()

  except Exception as err:
    logging.error("get_image_labels():")
    logging.error(str(err))

    raise

  finally:
    pass

###################################################################
#
# get_images_with_label
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def get_images_with_label(label):
  """
  When an image is uploaded to S3, the Rekognition AI service is
  automatically called to label objects in the image. These labels
  are then stored in the database for retrieval / search. Given a
  label (partial such as 'boat' or complete 'sailboat'), this 
  function performs a case-insensitive search for all images with
  this label. The function returns a list of images, where each 
  element of the list is a tuple of the form (assetid, label, 
  confidence). The list is returned in order by assetid, and for
  all elements with the same assetid, ordered by label. If an 
  error occurs, an exception is raised.

  Parameters
  ----------
  label to search for, this can be a partial word (e.g. 'boat')

  Returns
  -------
  a list of images that contain this label, even partial matches.
  Each element of the list is a tuple (assetid, label, confidence)
  where assetid identifies the image, label is a string, and 
  confidence is an integer. The list is returned in order by 
  assetid, and for all elements with the same assetid, ordered
  by label. If an error occurs, an exception is raised.
  """

  try:
    url = _build_url("/images_with_label")

    params = {'label': label}

    response = requests.get(url, params=params)

    if response.status_code == 200:
      body = _safe_json(response)
      #
      # success
      #
      rows = body['data']

      labels = []

      for row in rows:
        assetid = row["assetid"]
        label = row["label"]
        confidence = row["confidence"]
        labels.append((assetid, label, confidence))

      return labels

    elif response.status_code == 400:
      body = _safe_json(response)
      msg = body['message']
      raise ValueError(msg)

    elif response.status_code == 500:
      body = _safe_json(response)
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      raise HTTPError(err_msg)

    else:
      #
      # Any other error (404, etc.)
      #
      response.raise_for_status()

  except Exception as err:
    logging.error("get_images_with_label():")
    logging.error(str(err))

    raise

  finally:
    pass

###################################################################
#
# delete_images
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def delete_images():
  """
  Delete all images and associated labels from the database and 
  S3. Returns True if successful, raises an exception on error.
  The images are not deleted from S3 unless the database is 
  successfully cleared; if an error occurs either (a) there are
  no changes or (b) the database is cleared but there may be
  one or more images remaining in S3 (which has no negative 
  effect since they have unique names).

  Parameters
  ----------
  N/A

  Returns
  -------
  True if successful, raises an exception on error
  """

  try:
    url = _build_url("/images")

    response = requests.delete(url)
    body = _safe_json(response)

    if response.status_code == 200:
      #
      # success
      #
      return True
    else:
      #
      # failed:
      #
      msg = body['message']
      err_msg = f"status code {response.status_code}: {msg}"
      #
      # NOTE: this exception will not trigger retry mechanism, 
      # since we reached the server and the server-side failed, 
      # and we are assuming the server-side is also doing retries.
      #
      raise HTTPError(err_msg)

  except Exception as err:
    logging.error("delete_images():")
    logging.error(str(err))
    #
    # raise exception to trigger retry mechanism if appropriate:
    #
    raise

  finally:
    # nothing to do
    pass
