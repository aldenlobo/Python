import logging 
 
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import xmlrpclib
import pickle
import errno
from xmlrpclib import Binary

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):

    def __init__(self,Qr,Qw,meta_port, data_port):
        self.fd = 0
        now = time()
        self.Qr = int(Qr)
        self.Qw = int(Qw)

        self.meta_server = xmlrpclib.ServerProxy("http://localhost:" + meta_port)
        self.data_server = []
        for i in data_port:
            self.data_server.append(xmlrpclib.ServerProxy("http://localhost:" + i))
        self.put_meta_server('/' , dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, list_nodes=[]))


    def get_meta_server(self,path):
        #print path
        # print (pickle.loads(self.meta_server.get(Binary(path))["value"].data))
        #print ("METADATA HERE    ",self.meta_server.get(Binary(path)))
        #print "********", pickle.loads(self.meta_server.get(Binary(path))["value"].data)   
        return pickle.loads(self.meta_server.get(Binary(path))["value"].data)

    def put_meta_server(self,path,meta):
        return self.meta_server.put(Binary(path),Binary(pickle.dumps(meta)),3000)

    def get_data_server(self,path):				
        serv_data = {}
        i = 0
        majority = 0		#To check for the most common data.
        if self.Qr % 2 == 0:
            majority = (self.Qr/2)+1
        else:
            majority = (self.Qr+1)/2
        # while len(serv_data)<(self.Qr):
        for i in range(self.Qw):
            try:
                serv_data[i] = (pickle.loads(self.data_server[i].get(Binary(path))["value"].data))
            except Exception, E:
                print E
                print ('Data Server %i is not reachable.' %(i+1)) 
        print len(serv_data)
        if len(serv_data)<(self.Qr):
            return "Data cannot be accessed."        #Checks if all the servers have 
                                #have written & then compare if it satisfies the Qr condition.
        unique = list(set(serv_data.values()))	#Creates a list of unique datas. 
        unique_count = [0]*len(unique)
        for i in range(0,len(unique)):          #Unique_count is popullated with the actual count of the respective data.
            #unique_count[unique.index(i)] += 1
            unique_count[i] = serv_data.values().count(unique[i])
        max_value = max(unique_count)
        # if max_value >= self.Qr:           #Compares the data count obtained with that of majority to see if it satisfies the quorum requirement.
        #     return unique[unique_count.index(max_value)]
        # else:
        #     print("Please try again")
        #     return False

        match = self.Qr
        if max_value >= match:          #Used to check if data is different & then repair if it is.
            cor_data = unique[unique_count.index(max_value)]
            for k,v in serv_data.items():
                if v != cor_data:
                    self.data_server[k].repair(Binary(path))
            return cor_data
        else:
            print "ERROR!!"
            return ""

        
    def put_data_server(self,path,data):
        count = 0
        put_data = []
        for i in self.data_server:
            try:
                put_data.append(i.put(Binary(path),Binary(pickle.dumps(data)),3000))
                count +=1
            except: 
                raise FuseOSError(errno.ECONNREFUSED)
                break
        if count == self.Qw:        #Checks to see if it satisfies the write quorum requirement. 
            return True
        else:
            return False

 #    def trav_full(self, path):
 #        a = path.split('/')
    # pathlen = len(a)
    # temp = self.files['/']
 #        if not path == '/':
 #            for i in range(1, pathlen):
 #                temp = temp[a[i]]
 #        return temp

    def path_parent(self, path):        #Used to obtain the name of the parent.
        r = path.rfind("/")
        if r == 0:
            return "/"
        else: 
            return path[:r]

    def path_child(self,path):          #Used to obtain the name of the child.
      a = path.split('/')
      child = a[-1]
      return child  

    def del_all(self,path):         #Used to remove the data and meta-data for the path provided.       
        for i in self.data_server: 
            i.rmv(Binary(path))
        self.meta_server.rmv(Binary(path))

    def chmod(self, path, mode):
        trav1 = self.get_meta_server(path)
        trav1['st_mode'] &= 0770000
        trav1['st_mode'] |= mode
        self.put_meta_server(path,trav1)
        return 0

    def chown(self, path, uid, gid):
        trav1 = self.get_meta_server(path)
        trav1['st_uid'] = uid
        trav1['st_gid'] = gid
        self.put_meta_server(path,trav1)

    def create(self, path, mode):
        # a = path.split('/')
        # pathlen = len(a)
        # trav2 = self.(path)
        d='' 
        if self.put_data_server(path,d) == False:
            return 0
        m = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        self.put_meta_server(path,m)
        #print 'pp', self.path_parent(path)
        temp = self.get_meta_server(self.path_parent(path))
        temp['list_nodes'].append(self.path_child(path))
        self.put_meta_server(self.path_parent(path), temp)
        self.fd += 1      
        return self.fd

    def getattr(self, path, fh=None):
        try:
            temp=self.get_meta_server(path)             
        except KeyError:
            raise FuseOSError(ENOENT)
        return temp

    def getxattr(self, path, name, position=0):

        try:
            attrs = self.get_meta_server(path).get('attrs', {})
            return attrs[name]
        except KeyError:
            return ''      

    def listxattr(self, path):
        attrs = self.get_meta_server(path).get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        m = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),list_nodes=[])
        self.put_meta_server(path,m)
        temp = self.get_meta_server(self.path_parent(path))
        temp['st_nlink'] += 1
        temp['list_nodes'].append(self.path_child(path))
        self.put_meta_server(self.path_parent(path),temp)

        
    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.get_data_server(path)[offset:offset + size]
         
    def readdir(self, path, fh):
        return ['.', '..'] + self.get_meta_server(path)['list_nodes']  

    def readlink(self, path):
        return self.get_data_server(path)

    def removexattr(self, path, name):
        attrs = self.get_meta_server(path).get('attrs', {})
        #m = attrs.get('attrs', {})
        try:
            del attrs[name]
            # del m[name]
            # attrs["attrs"] = m
        except KeyError:
            pass        # Should return ENOATTR
        self.put_meta_server(path,attrs)        


    def rename2(self,old,new):          #Called by the rename() function for recussion. 
        m = self.get_meta_server(old)
        if ((m['st_mode'] & 0770000 != S_IFDIR)) :  
            d = self.get_data_server(old)
            self.put_data_server(new, d)
        self.meta_server.rmv(Binary(old))

        parent_old =  self.path_parent(old)
        g = self.get_meta_server(parent_old)
        g['list_nodes'].remove(self.path_child(old))
        self.put_meta_server(parent_old, g)

        parent_new = self.path_parent(new)
        h = self.get_meta_server(parent_new)
        h['list_nodes'].append(self.path_child(new))
        self.put_meta_server(parent_new, h)

        if(parent_old != parent_new):
            self.rename2(parent_old, parent_new)

    def rename(self, old, new):
        print old
        print new
        m = self.get_meta_server(old)
        self.put_meta_server(new, m)
        if ((m['st_mode'] & 0770000 == S_IFDIR)) : 
            if(len(m['list_nodes'])>0):
                for i in m['list_nodes'] :
                    part1 = old + '/' + i
                    part2 = new + '/' + i
                    self.rename(part1, part2)
        self.rename2(old,new)

        
    def rmdir(self, path):
        m = self.get_meta_server(path)
        if (m['st_mode'] & 0770000 != S_IFDIR):		#Checks if its not a dir.
            raise FuseOSError(ENOTDIR)
        if len(m['list_nodes']) > 0:				#Checks if its empty.
            raise FuseOSError(ENOTEMPTY)        
        self.meta_server.rmv(Binary(path))
        m = self.get_meta_server(self.path_parent(path))
        m['list_nodes'].remove(self.path_child(path))
        m['st_nlink'] -= 1
        self.put_meta_server(self.path_parent(path),m)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        #attrs = self.get_meta_server(path)
        # m = attrs.setdefault('attrs', {})
        # m[name] = value
        # attrs["attrs"]=m
        attrs = self.get_meta_server(path).setdefault('attrs', {})
        attrs[name] = value
        self.put_meta_server(path,attrs)

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.put_meta_server(target,dict(st_mode=(S_IFLNK | 0777), st_nlink=1,st_size=len(source)))
        trav2 = self.path_parent(target)
        m = self.get_meta_server(trav2)
        m['list_nodes'].append(self.path_child(target))
        self.put_meta_server(trav2,m)
        self.put_data_server(target,source)

    def truncate(self, path, length, fh=None):
        #self.data[path] = self.data[path][:length]
        m = self.get_data_server(path)
        m = m[:length]
        self.put_data_server(path,m)
        temp = self.get_meta_server(path)
        temp['st_size'] = length
        self.put_meta_server(path,temp)

    def unlink(self, path):
        #self.data_server.rmv(path)  
        self.meta_server.rmv(Binary(path))
        trav2 = self.path_parent(path)
        m = self.get_meta_server(trav2)
        m['list_nodes'].remove(self.path_child(path))
        self.put_meta_server(trav2,m)
        self.del_all(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        temp = self.get_meta_server(path)
        temp['st_atime'] = atime
        temp['st_mtime'] = mtime
        self.put_meta_server(path,temp)

    def write(self, path, data, offset, fh):      
        d = self.get_data_server(path)
        d = d[:offset] + data
        self.put_data_server(path,d)
        m = self.get_meta_server(path)
        m['st_size'] = len(d)     
        self.put_meta_server(path,m)
        return len(data)
    

if __name__ == '__main__':
    if len(argv) < 6:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    if (int(argv[2])*int(argv[3]))	 < 1:        #Checks if a valid Qr & Qw are provided.
    	print ('The Qr & Qw values are not acceptable.')
    	exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(argv[2],argv[3],argv[4],argv[5:]), argv[1], foreground=True)
