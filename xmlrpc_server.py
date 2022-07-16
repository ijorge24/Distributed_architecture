from distutils.command.config import config
from xmlrpc.server import SimpleXMLRPCServer
import logging
import xmlrpc.client
import config
import pandas as pd
import threading
import redis
import time

# Variables
headers=['Local time','Ask','Bid','AskVolume','BidVolume']
array_ip=[]
global rol
r = redis.Redis('localhost')
r.publish('channel-1', 'inicio')
try:
   p_cluster = r.get("cluster").decode("utf-8")
   proxy = xmlrpc.client.ServerProxy('http://localhost:'+p_cluster)
   host=proxy.get_next_host(p_cluster)
   rol = 'worker'
   print(proxy.put_server(host),rol)
   host_link="http://localhost:"+str(host)
   # Set up logging
   logging.basicConfig(level=logging.INFO)
   server = SimpleXMLRPCServer(
      ('localhost', int(host)),
      logRequests=True,
)
   #r.publish('channel-1', 'new_node')
   array_ip=proxy.get_servers()
   for position in array_ip:
      if position != host:
         servidor = xmlrpc.client.ServerProxy('http://localhost:' + str(position))
         print(servidor.put_server(host))
except:
   rol='cluster'
   print('im cluster', str(config.CLUSTER))
   r.mset({rol: str(config.CLUSTER)})
   # Set up logging
   logging.basicConfig(level=logging.INFO)
   server = SimpleXMLRPCServer(
      ('localhost', config.CLUSTER),
      logRequests=True,
)
   

# ----FUNCIONES DE CLUSTER
def get_next_host(value):
   value = int(value) +1
   if not array_ip:
      print (value)
      return str(value)
   else:
      host = array_ip[-1]
      host = int(host) + 1
      return host

server.register_function(get_next_host)


def put_server(server_name):
   add_server(server_name)
   return server_name

server.register_function(put_server)


def take_server(server_name):
   remove_server(server_name)
   r.publish('channel-1', 'nodes-1')
   return server_name

server.register_function(take_server)


def close_server(server_name):
   remove_server(server_name)
   print(server_name, " disconnected")
   return server_name

server.register_function(close_server)


# ------FUNCIONES PARA CLIENTE
# listar direcciones, devuelve las direcciones conectadas
def get_servers():
   return array_ip.copy()

server.register_function(get_servers)


#### funciones del cluster propias

def add_server(url):
   array_ip.append(url)

server.register_function(add_server)  


def remove_server(url):
   array_ip.remove(url)

server.register_function(remove_server)


def remove_all(url):
   array_ip.remove(url)
   for position in array_ip:
         servidor = xmlrpc.client.ServerProxy('http://localhost:' + str(position))
         print(servidor.take_server(url))

   server.register_function(remove_all)


def ping():
   return True

server.register_function(ping)


def ping_servers():
   print(array_ip)
   for position in array_ip:
      try:
         servidor = xmlrpc.client.ServerProxy('http://localhost:' + str(position))
         servidor.ping()
      except:
         print("fallen server:", position)
         remove_all(position) 

server.register_function(ping_servers)


#DEFINICIONES DE WORKER
def get_min(csv_file, since, to, price):
   since=int(since)
   to=int(to)
   df=pd.read_csv(csv_file, skiprows=since, nrows=to)
   df.columns = headers
   min=df[price].min()
   print(min)
   return str(min)

server.register_function(get_min)


def get_max(csv_file, since, to, price):
   since=int(since)
   to=int(to)
   df=pd.read_csv(csv_file, skiprows=since, nrows=to)
   df.columns = headers
   max=df[price].max()
   print(max)
   return str(max)

server.register_function(get_max)


def ping_cluster():
   global rol
   try:
      p_cluster = r.get("cluster").decode("utf-8")
      proxy = xmlrpc.client.ServerProxy('http://localhost:'+p_cluster)
      proxy.ping()
   except:
      print("fallen cluster:")
      print("change cluster to:", host)
      r.mset({"cluster": array_ip[0]})
      check=r.get("cluster").decode("utf-8")
      if str(host)==check:
         rol='cluster'
      remove_all(host)    

server.register_function(ping_cluster)


try:
   while 1:
      start = threading.Thread(target=server.serve_forever, daemon=True)
      start.start()
      if rol == 'worker':
         if str(host) == str(array_ip[0]):
            print("first worker")
            ping_cluster()
#        array_ip=proxy.get_servers()       #pull solution
         print('workers conected:' ,array_ip)
#     server.serve_forever()
      else:
         ping_servers()

      time.sleep(2)
except KeyboardInterrupt:
   if rol == 'worker':
      close_server(host)
   else:
      r.mset({rol: str(0)})
   server.shutdown()
   print('Exiting')


########################################################################################