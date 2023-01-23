import json
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import uuid
import time

import constants
from api import json_api

class ThreadingServer(ThreadingMixIn, HTTPServer):
    pass

# TODO support multiple simultaneous commands to one target (dicts should contain lists of requests, not individual requests)
# Make it async using aiohttp?
# ThreadingMixIn?
class Handler(BaseHTTPRequestHandler):
    def __init__(self, queries_dict: dict, responses_dict, *args, **kwargs):
        self._queries_dict = queries_dict
        self._responses_dict = responses_dict
        super().__init__(*args, **kwargs)
    
    def do_POST(self):
        request_uuid = str(uuid.uuid4())
        content_length = int(self.headers['Content-Length'])
        content = self.rfile.read(content_length).decode('utf-8')

        json_dict = json.loads(content)
        json_dict['request_uuid'] = request_uuid
        json_str = json.dumps(json_dict, ensure_ascii=False)
        target = json_api.get_target(json_dict)
        print(target)
        self._queries_dict[target] = json_str
        # TODO make this async
        #print(self._queries_dict)
        while request_uuid not in self._responses_dict:
            time.sleep(0.1)
        response: str = self._responses_dict[request_uuid]
        response_success = response[0]
        if response_success == '1':
            self.send_response(200)
        else:
            self.send_response(400)
        # TODO
        #self.send_header("Content-type", "text/html")
        self.end_headers()
        # TODO
        self.wfile.write(response[1:].encode())
        #self.wfile.close()



# queries_dict – multiprocessing.manager dict
def start_handler(queries_dict: dict, responses_dict: dict):
    print('Starting server...')
    #global server
    #server = HTTPServer(('127.0.0.1', 8000), Handler)
    # Source: https://stackoverflow.com/a/52046062
    handler = partial(Handler, queries_dict, responses_dict)
    with HTTPServer(('127.0.0.1', constants.SERVER_PORT), handler) as server:
        server.serve_forever()
