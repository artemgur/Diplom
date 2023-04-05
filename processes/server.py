import json
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import uuid
import time

import constants
import queries_dict_manager
from api import json_api

class ThreadingServer(ThreadingMixIn, HTTPServer):
    pass

# Make it async using aiohttp?
# ThreadingMixIn?
class Handler(BaseHTTPRequestHandler):
    def __init__(self, queries_dict: dict, responses_dict, view_names_dict, *args, **kwargs):
        self._queries_dict = queries_dict
        self._responses_dict = responses_dict
        self._view_names_dict = view_names_dict
        super().__init__(*args, **kwargs)
    
    def do_POST(self):
        request_uuid = str(uuid.uuid4())
        # For test_multiple_clients.py
        #print('Started handling query', request_uuid)
        content_length = int(self.headers['Content-Length'])
        content = self.rfile.read(content_length).decode('utf-8')

        json_dict = json.loads(content)
        json_dict['request_uuid'] = request_uuid
        json_str = json.dumps(json_dict, ensure_ascii=False)
        target = json_api.get_target(json_dict)
        # For test_multiple_clients.py
        #print('    Target', target, 'request uuid', request_uuid)
        queries_dict_manager.add(self._queries_dict, target, json_str)
        #self._queries_dict[target] = json_str
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
        # For test_multiple_clients.py
        #print('Finished handling query', request_uuid)



# queries_dict – multiprocessing.manager dict
def start_handler(queries_dict: dict, responses_dict: dict, view_names_dict: dict):
    print('Starting server...')
    #global server
    #server = HTTPServer(('127.0.0.1', 8000), Handler)
    # Source: https://stackoverflow.com/a/52046062
    handler = partial(Handler, queries_dict, responses_dict, view_names_dict)
    with ThreadingServer(('127.0.0.1', constants.SERVER_PORT), handler) as server:
        server.serve_forever()
