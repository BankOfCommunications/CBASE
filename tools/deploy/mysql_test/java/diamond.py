#!/usr/bin/env python2.6

''' diamond mock '''

# author: junyue 
# date  : 2012-12-05

import sys,urllib,re 
from os import curdir,sep,path,makedirs
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer


class DiamondHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            method=False
            key='mytest'
            value=''            
            kw=self.path.split("&")
	    file=True
            for c in kw:
                 k = c.split("=")
                 if k[0] == '/method' and k[1] == 'set':
                    method='set'
		 elif k[0] == '/method' and k[1] =='get':
		    method='get'
                 elif k[0] == 'key':
		    k[1]=k[1].strip()
		    special_file=['diamond.py','nohup.out','Makefile','Makefile.am','Makefile.in','configure','configure.ac','build.sh','bin','src']
		    p=re.compile('^([0-9a-zA-Z_]|-|\.)+$')
		    match=p.match(k[1])
		    if match and  k[1] not in special_file :
                        key=k[1]	
		    else:
			file=False
		 elif k[0] == 'value':
                    value=k[1]
	    if file == False:
                self.send_error(400, 'File Is Invalid: %s' % self.path)
            elif method  == 'get' :
		f=open(curdir+sep+'value_data'+sep+key)
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                self.wfile.write(f.read())
                f.close()
            elif method =='set':
		f=open(curdir+sep+'value_data'+sep+key, "w")
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                content = urllib.unquote(value)
                f.write(content)
                f.close()
                self.wfile.write('ok')
	    else:
		self.send_error(400, 'Invalid Method: %s' % self.path)
                
        except IOError:
            self.send_error(404, 'File Not Found: %s' % self.path)
            
if __name__=='__main__':

    try:
	if not path.exists('./value_data'):
	   makedirs('./value_data')
        server = HTTPServer(('',9000),DiamondHandler)
        print 'start HTTPServer'
	server.serve_forever()
    except KeyboardInterrupt:
        print 'received ,shutting down Server'
	server.socket.close()

