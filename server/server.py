import BaseHTTPServer
import urlparse

class SimpleRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        print self.path
        #self.wfile.write('HTTP/1.0 200 Okay\r\n\r\n   ')
        self.wfile.write('HTTP/1.0 200 Okay\r\n\r\nHere is your output for '+self.path)

    def do_POST(self):
	length = int(self.headers.getheader('content-length'))
        field_data = self.rfile.read(length)
        fields = urlparse.parse_qs(field_data)
        print field_data
        self.wfile.write('HTTP/1.0 200 Okay\r\nContent-Length: 42\r\nContent-Type: application/json\r\n\r\n{"completion_url": "http://host:port/uri"}')
        #self.wfile.write('   ')
        #return {
#  "completion_url": "http://host:port/uri"
#}
 #       return {
 #           'TESTE': 'TESTE'
 #       }

def run(server_class=BaseHTTPServer.HTTPServer,
    handler_class=SimpleRequestHandler):
    server_address = ('', 8500)
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()

run()
