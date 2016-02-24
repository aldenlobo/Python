import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from sys import argv
from SimpleXMLRPCServer import SimpleXMLRPCServer

import xmlrpclib
import pickle
from xmlrpclib import Binary

server_url = "http://localhost:33333/"
server = xmlrpclib.ServerProxy(server_url)

server.print_content()
#print server.list_contents()
#server.corrupt(Binary("/s"))
#server.terminate()
#server.restart(22224)
#server.terminate()
server.print_content() 