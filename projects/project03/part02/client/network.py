#
# Networking utilities:
#
import subprocess
import socket
import re
import logging
import uvicorn
from fastapi import FastAPI, Request


#
# find a free network port to use in the
# range 8001..8010, and return that port
# number. 
#
# returns -1 if no port is available
#
def find_free_port():
    ports = range(8001, 8011) # 8001, 8002, ..., 8010
    for port in ports:
      with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            return port
        except Exception as err:
            # try next port
            pass
    #
    # if get here, all ports are in use:
    #
    return -1


#
# Starts cloudflare tunnel needed so that web 
# service can "tunnel back" to this computer
# from anywhere on the internet without having
# to worry about local network configuration:
#
def start_cloudflare_tunnel(port):
    #
    # Launch cloudflared as a subprocess and parse 
    # its output to extract the ephemeral public 
    # URL that represents the callback URL to this
    # app:
    #
    logging.info(f"Creating tunnel on port {port}")
    print(f"Creating tunnel on port {port}...")

    proc = subprocess.Popen(
        ["cloudflared", "tunnel", "--url", f"http://localhost:{port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    #
    # Cloudflare ephemeral URLs look like:
    #   https://random-name.trycloudflare.com
    #
    url_pattern = re.compile(r"https://[-a-zA-Z0-9]+\.trycloudflare\.com")

    for line in proc.stdout:
        # print(f"[cloudflared] {line.strip()}")
        m = url_pattern.search(line)
        if m:
            public_url = m.group(0)
            if public_url.startswith("https://api.trycloudflare.com"):
                pass
            else:
                logging.info(f"Tunnel URL: {public_url}")
                return public_url, proc

    raise RuntimeError("Cloudflare tunnel did not produce a public URL")


#
# start our internal web server (uvicorn) running 
# so we can receive callbacks:
#
def run_uvicorn(app, port):
    uvicorn.run(app, 
                host="0.0.0.0", 
                port=port, 
                log_level="error"
    )
