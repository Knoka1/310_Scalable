#
# Client-side chat app
#
# Author:
#   Prof. Joe Hummel
#   Northwestern University
#
import time
import threading
import sys
import logging
import requests
from fastapi import FastAPI, Request
from getpass import getpass
from configparser import ConfigParser
from requests.exceptions import HTTPError, ConnectionError, Timeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import network  # our networking utilities

#
# Module-level variables
#
USERNAME = "?"
AUTH_SERVICE_URL = "?"
CHAT_SERVICE_URL = "?"

app = FastAPI()

#
# Callback functions ("webhooks"):
#
@app.get("/displayname")
def get_displayname(request: Request):
    #
    # provides chatapp with our user name for 
    # display purposes:
    #
    return {"message": "success", 
            "displayname": USERNAME}

@app.post("/message")
async def post_message(request: Request):
    #
    # allows chatapp to post a message from another 
    # client:
    #
    try:
        await request.body()
        body = await request.json()
        
        print()
        print(f"From {body["displayname"]}:")
        print(body["message"])
        print()
        
        return {"message": "success"}
    
    except Exception as err:
        logging.error(f"/message: {str(err)}")
        return {"message": str(err)}
    

#
# register
#
# Registers our callback functions with the web
# service.
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def register(callback_url, token, authsvc_to_use):

    try:
        displaynamehook = callback_url + "/displayname"
        messagehook = callback_url + "/message"

        # 
        # authentication information is passed in the
        # request header so that it works with any HTTP
        # verb GET, PUT, POST, etc.
        #
        # NOTE: must pass strings.
        #
        req_header = {"Authentication": str(token),
                      "Service": str(authsvc_to_use)
                     }

        #
        # remaining parameters go in request body:
        #
        body = {"displaynamehook": displaynamehook,
                "messagehook": messagehook
               }
        
        url = CHAT_SERVICE_URL + "/register"
        response = requests.put(url, headers=req_header, json=body)

        if response.status_code == 200:
            # success!
            return
        elif response.status_code in [400, 500]:
            msg = response.json()
            raise HTTPError(msg)
        else:
            # 
            # something unexpected happened, so have Python raise
            # proper HTTPError for us:
            #
            response.raise_for_status()
            
    except Exception as err:
        logging.error("register():")
        logging.error(str(err))
        #
        # raise exception to trigger retry mechanism if appropriate:
        #
        raise

    finally:
        # nothing to do
        pass


#
# message
#
# Posts a message to the chat web service to send to all
# registered clients (including ourselves).
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def message(token, authsvc_to_use, msg):
    #
    # TODO #2:
    #
    print("**TODO 2 of 3: POST /message")
    return

    try:
        # 
        # authentication information is passed in the
        # request header so that it works with any HTTP
        # verb GET, PUT, POST, etc.
        #
        # NOTE: must pass strings.
        #
        req_header = {"Authentication": str(token),
                      "Service": str(authsvc_to_use)
                     }

        #
        # remaining parameters go in request body:
        #
        body = {"message": msg}
        
        url = CHAT_SERVICE_URL + "/message"
        response = requests.post(url, headers=req_header, json=body)

        if response.status_code == 200:
            # success!
            return
        elif response.status_code in [400, 500]:
            msg = response.json()
            raise HTTPError(msg)
        else:
            # 
            # something unexpected happened, so have Python raise
            # proper HTTPError for us:
            #
            response.raise_for_status()
            
    except Exception as err:
        logging.error("message():")
        logging.error(str(err))
        #
        # raise exception to trigger retry mechanism if appropriate:
        #
        raise

    finally:
        # nothing to do
        pass


#
# delete_registration
#
# Deletes our registration with the web service.
#
@retry(stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, Timeout)),
        reraise=True
      )
def delete_registration(token, authsvc_to_use):
    #
    # TODO #3:
    #
    print("**TODO 3 of 3: DELETE /register")
    return

    try:
        # 
        # authentication information is passed in the
        # request header so that it works with any HTTP
        # verb GET, PUT, POST, etc.
        #
        # NOTE: must pass strings.
        #
        req_header = {"Authentication": str(token),
                      "Service": str(authsvc_to_use)
                     }
        
        url = CHAT_SERVICE_URL + "/register"
        response = requests.delete(url, headers=req_header)

        if response.status_code == 200:
            # success!
            return
        elif response.status_code in [400, 500]:
            msg = response.json()
            raise HTTPError(msg)
        else:
            # 
            # something unexpected happened, so have Python raise
            # proper HTTPError for us:
            #
            response.raise_for_status()
            
    except Exception as err:
        logging.error("delete_registration():")
        logging.error(str(err))
        #
        # raise exception to trigger retry mechanism if appropriate:
        #
        raise

    finally:
        # nothing to do
        pass
    
    
#
# login
#
# Prompts the user to login, and repeats until successful or
# the user quits the program with Ctrl-C. Returns the authentication
# token when successful.
#
# NOTE: we don't need a retry mechanism because the user is our
# retry mechanism in this case.
#
def login():
    #
    # login user, repeat until successful:
    #
    while True:
        global USERNAME
        USERNAME = input("Enter username: ")
        password = getpass()
        duration = input("# of minutes before expiration? [ENTER for default] ")

        #
        # build message:
        #
        if duration == "":  # use default
            body = {"username": USERNAME, "password": password}
        else:
            body = {"username": USERNAME, "password": password, "duration": duration}

        #
        # call the web service to upload the PDF:
        #
        url = AUTH_SERVICE_URL + "/auth"

        response = requests.post(url, json=body)

        password = None  # clear variable for security reasons

        if response.status_code == 200:
            # success!
            token = response.json()
            return token
        elif response.status_code in [400, 401, 500]:
            msg = response.json()
            logging.error(f"trying to login: {msg}")
            print(f"Error trying to login: {msg}")
            print()
        else:
            print(f"Unknown error trying to login, status code: {response.status_code}")
            print()

#
# read_config_files
#
# Reads the client-side config files to obtain
# URLs to auth service and chat web service.
# Pass 1 to use YOUR auth service, and 2 to use
# STAFF auth service.
#
def read_config_files(authsvc):
    global AUTH_SERVICE_URL
    global CHAT_SERVICE_URL

    if authsvc == 1:
        authsvc_config_file = "authsvc-client-config.ini"
    else:
        authsvc_config_file = "authsvc-client-config-staff.ini"

    configur = ConfigParser() 
    configur.read(authsvc_config_file)
    AUTH_SERVICE_URL = configur.get('client', 'webservice')

    chatapp_config_file = "chatapp-client-config.ini"
    configur.read(chatapp_config_file)
    CHAT_SERVICE_URL = configur.get('client', 'webservice')


#################################################
# main
################################################
print("**starting**")
print("**use Ctrl-C if you need to quit at any time...")
print()

#
# eliminate traceback so we just get error message:
#
sys.tracebacklimit = 0

#
# capture logging output in file 'log.txt'
#
logging.basicConfig(
  filename='log.txt',
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s',
  filemode='w'
)

#
# find a free network port for our app to use:
#
port = network.find_free_port()
if port < 0: 
    # no ports available:
    logging.error("no ports available")
    print()
    print("**ERROR: no ports available, exiting...")
    print()
    sys.exit(0)

logging.info(f"FastAPI starting on port {port}")

#
# start uvicorn web server in a background thread, 
# which runs our embedded web service supporting 
# callbacks from the chatapp web service:
#
threading.Thread(target=network.run_uvicorn, 
                 args=(app,port,), 
                 daemon=True
                ).start()

#
# start Cloudflare tunnel so chatapp can reach
# us from anywhere on the internet, without us 
# having to configure local network settings.
#
# After call, public_url denotes unique URL back
# to this client, forming the baseurl for our 
# callback functions. Also sets a process reference
# so tunnel can be shutdown when client app ends.
#
try:
    public_url, tunnel_proc = network.start_cloudflare_tunnel(port)
except Exception as err:
    msg = str(err)
    logging.error(msg)
    print()
    print("**ERROR: unable to start tunnel")
    print(f"**ERROR: '{msg}', exiting...")
    print()
    sys.exit(0)

print(f"Tunnel URL to this client: {public_url}")
print("**ready**")
print()

#
# read config files to get URLs to auth service
# and chat web service:
#
authsvc = 1  # use 1 for YOUR auth service, 2 for STAFF auth service:
read_config_files(authsvc)

#
# Now run the client UI:
#
try:
    token = login()
        
    # 
    # if get here, we are successfully logged in:
    #
    print()
    print("**successfully logged in, token:", token)

    print("**deleting any prior registration...")
    delete_registration(token, authsvc)

    print("**registering callbacks...")
    register(public_url, token, authsvc)
    print("**success")
    print()

    msg = input("Enter msg to send: [ENTER to quit] ")
    while msg != "":
        #
        # post message to web service:
        #
        message(token, authsvc, msg)
        
        msg = input("Enter msg to send: [ENTER to quit] ")

except KeyboardInterrupt:
    pass
except Exception as err:
    msg = str(err)
    logging.error(msg)
    print()
    print(f"**ERROR: '{msg}'")
    print()    
finally:
    print()
    print("**deleting registration...")
    try:
        delete_registration(token, authsvc)
    except:
        pass
    print("**shutting down tunnel...")
    tunnel_proc.terminate()

print("**done**")
