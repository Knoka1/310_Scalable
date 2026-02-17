#
# Calls S3 to download images from bucket
#
# Prof. Joe Hummel
# Northwestern University
#

import requests
import time
from configparser import ConfigParser
import xml.etree.ElementTree as ET

print("**Starting**")
print()

#
# setup AWS based on config file:
#
config_file = 's3-config.ini'    
configur = ConfigParser()
configur.read(config_file)

#
# get web server URL from config file:
#
endpoint = configur.get('webserver', 'endpoint')

#
# Call S3 web server to download image requested by user:
#
imagename = input("Enter image to download without extension> ")
# + ".azonaws.com.nu.cs"
url = endpoint + "/" + imagename
retry = 1
max_retries = 3

while retry <= max_retries:
  try:
    response = requests.get(url)
    if response.status_code in (200, 404):
      break
  except Exception as e:
    if retry == max_retries:
      response = requests.models.Response()
      response.status_code = -1
      root = ET.Element("Error")
      ET.SubElement(root, "Message").text = str(e)
      response._content = ET.tostring(root, encoding='UTF-8')
      break
  finally:
    if retry < max_retries:
      time.sleep(retry)
      retry += 1

#
# process the response:
#
status_code = response.status_code

print()
print('status code:', status_code)
print()

if status_code == 200:
  #
  # success, write image to a local file so we can view:
  #
  content_type = response.headers['Content-Type']
  parsed_content_type = "."
  if 'jpeg' in content_type:
    parsed_content_type += 'jpg'
  elif 'text/plain' in content_type:
    parsed_content_type += 'txt'
  elif 'python' in content_type:
    parsed_content_type += 'py'
  elif 'application' in content_type:
    parsed_content_type += content_type.split('/')[1]
  else:
    parsed_content_type += 'unknown'
  
  imagename = imagename + parsed_content_type
  file = open(imagename, 'wb')
  file.write(response.content)
  file.close()
  print(f"Success, image downloaded to '{imagename}'")
else:
  #
  # error:
  #
  xml_doc = ET.fromstring(response.text)
  print(f"ERROR:\n URL: {url} \n Msg: {xml_doc.find('Message').text}")

print()
print("**Done**")
