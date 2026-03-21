#
# PUT /register 
#
# Lambda function to register a client's display name
# and callback functions (webhooks).
#
import json
import requests
import api_utils
import datatier
import time

from configparser import ConfigParser

def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: register**")

        #
        # retrieve configuration info for MySQL access:
        #
        configur = ConfigParser()

        config_file = 'chatapp-config.ini'
        configur.read(config_file)
        
        rds_endpoint = configur.get('rds', 'endpoint')
        rds_portnum = int(configur.get('rds', 'port_number'))
        rds_username = configur.get('rds', 'user_name')
        rds_pwd = configur.get('rds', 'user_pwd')
        rds_dbname = configur.get('rds', 'db_name')

        #
        # (1) retrieve token and authsrv from request header:
        #
        token = ""
        authsvc = ""
        
        print("**Accessing request headers to get authentication info**")

        if "headers" not in event:
            return api_utils.error(400, "no headers in request")
        
        headers = event["headers"]
        
        if "Authentication" not in headers:
            return api_utils.error(400, "no authentication token in request headers")
        
        token = headers['Authentication']

        if "Service" not in headers:
            return api_utils.error(400, "no authentication service in request headers")
        
        authsvc = headers['Service']

        #
        # (2) retrieve callback URLs from request body:
        #
        print("**Accessing request body**")
        
        displaynamehook = ""
        messagehook = ""

        if "body" not in event:
            return api_utils.error(400, "no body in request")
        
        body = json.loads(event["body"])

        if "displaynamehook" in body:
            displaynamehook = body["displaynamehook"]
        else:
            return api_utils.error(400, "missing displaynamehook in body")

        if "messagehook" in body:
            messagehook = body["messagehook"]
        else:
            return api_utils.error(400, "missing messagehook in body")

        #
        # (3) is the token valid? Ask authentication service...
        #
        print('**Calling authentication service...')

        #
        # based on specified auth service, retrieve
        # the correct base URL from appropriate config
        # file:
        #
        if int(authsvc) == 1:
            configur.read('authsvc-client-config.ini')
        else:
            configur.read('authsvc-client-config-staff.ini')

        auth_url = configur.get('client', 'webservice')

        #
        # build request body and make the call:
        #
        # TODO: body = ...
        # TODO: response = requests.?(url, json=body)
        # TODO: if status code == 200:
        #         userid = ...
        #         print("Authentication successful, userid:", userid)
        #       elif status code == 401:
        #         api_utils.error(400, msg)
        #       elif status code in [400, 500]:
        #         api_utils.error(500, msg)
        #       else:
        #         api_utils.error(500, msg)
        #
        body = {"token": token}
        response = requests.post(auth_url + "/auth", json=body)
        if response.status_code == 200:
            userid = response.json()
            print("Authentication successful, userid:", userid)
        elif response.status_code == 401:
            return api_utils.error(401, response.json())
        elif response.status_code in [400, 500]:
            return api_utils.error(500, response.json())
        else:
            return api_utils.error(500, "unexpected response from authentication service")


        #
        # (4) let's callback and get display name:
        #
        print("**calling back to client to get display name...")
        print("displaynamehook:", displaynamehook)
        #
        # TODO: response.requests.?(url)
        #
        # body = response.json()
        # displayname = body["displayname"]
        # print("displayname callback returned this name:", displayname)
        response = requests.get(displaynamehook, headers={"User-Agent": "Mozilla/5.0"})
        body = response.json()
        displayname = body["displayname"]
        print("displayname callback returned this name:", displayname)



        #
        # (5) now let's post a message back to the client so they know:
        #
        print("**calling back to client to post message that they are successfully registered...")
        print("messagehook:", messagehook)
        #
        # TODO: body = ...
        # TODO: response.requests.?(url, json=body)
        #
        body = {"displayname": displayname, "message": "successfully registered as '" + displayname + "'"}
        response = requests.post(messagehook, json=body, headers={"User-Agent": "Mozilla/5.0"})



        #
        # (6) finally, let's register the client in the database
        # so we can post messages to them when they arrive. We need
        # to check first to see if the client is already registered,
        # and if so we update the existing information. Otherwise we
        # insert. This logic is necessary to honor the use of HTTP
        # PUT, which should be idempotent.
        #
        print("**Opening connection to database**")
        dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
        #
        # TODO: update or insert into DB
        #
        row = datatier.retrieve_one_row(dbConn, "SELECT userid FROM registered WHERE userid = %s", [userid])
        if row is None or row == ():
            datatier.perform_action(dbConn,
                "INSERT INTO registered(userid, displayname, displaynamehook, messagehook) VALUES(%s, %s, %s, %s)",
                [userid, displayname, displaynamehook, messagehook])
        else:
            datatier.perform_action(dbConn,
                "UPDATE registered SET displayname = %s, displaynamehook = %s, messagehook = %s WHERE userid = %s",
                [displayname, displaynamehook, messagehook, userid])



        #
        # success, done!
        #
        print("**DONE, returning success**")

        return api_utils.success(200, "success")

    except Exception as err:
        print("**ERROR**")
        print(str(err))

        return api_utils.error(500, str(err))

    finally:
        try:
            dbConn.close()
        except:
            pass