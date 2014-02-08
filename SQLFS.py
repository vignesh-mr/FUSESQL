from fuse import Fuse		#Get the Fuse class from fuse module
import fuse
import os,stat,urllib,errno,sys
import time
import SQL,utils

files=[]			# Represent path of the files
dirs={'/':[]}			# Represent directory structure as a key value python dictionary object
file_content={}			# Not necessary
file_mode={}			# The File Modes of each and every path, very important as getattr uses this
dirpath=['/']			# Not necessary
dir_mode={}			# The File Modes of each and every path, very important as getattr uses this


class MyStat(fuse.Stat):		#The resource statistics as given by fuse.Stat and set to values
	def __init__(self):
		self.st_mode=0
		self.st_nlink=0
		self.st_uid=0
		self.st_gid=0
		self.st_dev=0
		self.st_ino=0
		self.st_uid=0
		self.st_gid=0
		self.st_atime=0
		self.st_ctime=0
		self.st_mtime=0
		self.st_size=0

def SQLdir(Pdblist):
	utils.Logger('Pdblist',str(Pdblist))
	utils.Logger('Pdblist[0] type',str(type(Pdblist[0])))
	global dirs,dirpath,dir_mode			#Use the global variables
	folder = '/'					# Root of directory
	dblist = SQL.DB.listdatabases(Pdblist)		#Get the list of databases
	tbs = []
	st=MyStat()
	st.st_mode=stat.S_IFDIR| 0777		# this mode must be added as the gnome VFS needs a mode to access the root of the FS!!!
	now=time.time()
	st.st_atime=now
	st.st_mtime=now
	st.st_ctime=now
	dir_mode['/'] = st			#Set the root directory attributes
	for db in dblist:
		path = folder+str(db)		#/{dbname}	db contains name
		dirs[folder].append(path)
		dirpath.append(path)
		dirs[path] = []
		tabs = SQL.DB.listtabs(db,Pdblist)
		
		for tab in tabs:
			
			tbs.append(path+'/'+str(tab))
		st=MyStat()
		st.st_mode=stat.S_IFDIR|st.st_mode		#Set attributes for alll other directories or in this case Databases
		now=time.time()
		st.st_atime=now
		st.st_mtime=now
		st.st_ctime=now
		dir_mode[path] = st
	#print dblist
	#print dirs
	#print dir_mode
	#print dirpath
	return tbs,Pdblist	#return all tab names and dbobjects list as tuple

class FuseFS(Fuse):		#Base FUSE CLASS
	test=[]
	
	def __init__(self,version,usage,tbs):
		super(FuseFS,self).__init__(version=version,usage=usage)	#Use the superclass init function
		self.tbs,self.dbobjlist = tbs		#tab names and dbobjlist
		utils.Logger('self.dbobjlist',str(self.dbobjlist))
		for t in self.tbs:
			print t,type(t)
			self.mknod(t,stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH,0)	# set all the attribute values of the tables in each database
	
	def  getattr(self,path):#1
		'''Get the attributes of a resource'''
		st=MyStat()
		if path in dirpath:
			#st.st_mode=stat.S_IFDIR | 0777
			#st.st_nlink=2
			st=dir_mode[path]
		elif path in files:
			st=file_mode[path]
		else:
			return -errno.ENOENT
		return st
		
	def readdir(self,path,offset):#2
		'''Read the directory'''
		utils.Logger('ts',str(time.time())+' '+str(path))
		utils.Logger('offset',str(offset))
		global files, dirs, file_mode
		if path  != '/':
			db = path.split('/')[1]
			utils.Logger('dbname',str(db))
			dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
			utils.Logger('dbname object',str(type(dbobj)))
			dbobj.gettabs()			# get the tables in the database
			utils.Logger('get dbname tabs',str(db))
			utils.Logger('All files',str(files))
			
			tt=[l for l in files if path == l.rsplit('/',1)[0]]	#Get tabnames in that path
			utils.Logger('tables in db',str(tt))
			for t in tt:
				fullpath = str(t)
				utils.Logger('full path to table',str(fullpath))
				if fullpath in file_mode:
					utils.Logger('path exists in file modes',str(fullpath))
					file_mode[fullpath].st_size = dbobj.gettabsize(t)	#Set the size of file to tabsize
					utils.Logger('size set',str(fullpath))
		else:
			tt=[l for l in files if path == l.rsplit('/',1)[0]]	
		utils.Logger('tt',str(time.time())+' '+str(tt))
		g=[]
		for l in tt:
			if '/' in l:
				l=l.rsplit('/',1)[1]
				g.append(str(l))
		tt=g			#Set the table names to be shown
		utils.Logger('list of files',str(tt))
		for l in dirs[path]:
			utils.Logger('list of directories',str(l))
			l=l.rsplit('/',1)[1]
			tt.append(l)		#If there are any directories append that to the list
		#FuseFS.test.append(tt)
		tt.append('.')
		tt.append('..')
		utils.Logger('list of files and directories',str(tt))
		for r in tt:
			if r[0]!='.' and r[-1]!='~':
				yield fuse.Direntry(r)		#yield the data for each and every resource in the current directory

	'''def rmdir(self,path):#3
		for l in files:
			if path in l:
				return
		dirpath.remove(path)
		dir_mode.pop(path)
		dirs.pop(path)
		folder,n=path.rsplit('/',1)
		if folder=='':
			folder='/'
		dirs[folder].remove(path)

	

	def mkdir(self,path,mode):#5
		folder,name=path.rsplit('/',1)
		if folder=='':
			folder='/'
		dirpath.append(path)
		dirs[folder].append(path)
		dirs[path]=[]
		st=MyStat()
		st.st_mode=stat.S_IFDIR|mode
		now=time.time()
		st.st_atime=now
		st.st_mtime=now
		st.st_ctime=now
		dir_mode[path]=st

	def rename(self,old,new):#6
		if old in files:
			files.remove(old)
			files.append(new)
			c=file_content[old]
			file_content.pop(old)
			file_content[new]=c
			c=file_mode[old]
			file_mode.pop(old)
			file_mode[new]=c
		elif old in dirpath:
			dirpath.remove(old)
			dirpath.append(new)
			folder,name=old.rsplit('/',1)
			if folder=='':
				folder='/'
			dirs[folder].remove(old)
			dirs[folder].append(new)
			c=dirs[old]
			dirs.pop(old)
			dirs[new]=c
	'''
	
	
	def chmod(self,path,mode):#4
		'''Change the modes of a given resource in path'''
		global file_mode,dir_mode
		if path in file_mode:
			st=file_mode[path]
			st.st_mode=stat.S_IFREG|mode
			file_mode[path]=st
		elif path in dir_mode:
			st=dir_mode[path]
			st.st_mode=stat.S_IFDIR|mode
			dir_mode[path]=st
			
	def mknod(self,path,mode,dev):#7
		global files,file_content,file_mode
		now=time.time()
		files.append(path+'.csv')		#Show table as table.csv
		st=MyStat()
		st.st_mode=stat.S_IFREG | mode		#Set mode
		st.st_nlink=1
		st.st_ctime=now
		st.st_atime=now
		st.st_mtime=now
		file_mode[path+'.csv']=st
		file_content[path+'.csv']=''
		return 0

	def read(self, path, size, offset):#8	#path contains .csv extension
		'''Read a given file'''
		global file_mode
		utils.Logger('read path',str(path))
		table = path.rsplit('/',1)[1].split('.')[0]		#Get the table name alone from /path/table.csv
		db = path.split('/')[1]					#Get the database the table belongs to
        	utils.Logger('db path',str(db))
        	dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
        	utils.Logger('Got DB Obj',str(dbobj))
        	buff = dbobj.gettable(table)			# Get the table contents as string
        	head = dbobj.gettableheadings(table)		#Get the column headings as string
        	buff = head + buff				# Final string representation
        	utils.Logger('Got tab buf',str(buff))
        	slen = len(buff)
        	utils.Logger('Length of '+str(path),str(slen))
        	file_mode[path].st_size = slen
        	
		if offset < slen:
		    buf = buff[offset:offset+size]
		else:
		    buf = ''
		return str(buf)
	'''
	def unlink(self,path):#9
		global files,file_content,file_mode
		files.remove(path)
		file_content.pop(path)
		file_mode.pop(path)
	'''
	def write(self,path,data,offset):#10
		'''Write to a given file'''
		global file_content,files,file_mode
		utils.Logger('Write to file',str(path)+'>>>>>'+str(data))
		print 'DATA',data
		db = path.split('/')[1]			#Get the database the table belongs to
		table = path.rsplit('/',1)[1].split('.')[0]	#get the table name alone from /path/..../table.csv
		dbobj = SQL.DB.getobject(self.dbobjlist,db)
		dbobj.settabcontent(table,data)		#call the modificaiton function with the table name and data
		return len(data)			# return positive len(data) to show success
		'''
		st=file_mode[path]
		c=file_content[path]
		newc=data
		file_content[path]=newc
		st.st_size=len(file_content[path])
		file_mode[path]=st
		return len(data)
		'''
	def truncate(self,path,length):#11
		'''Truncate the file--- necessary for write function'''
		if path in files:		
			file_mode[path].st_size=length

	
	def utimens(self,path,times=None):#12		
		'''set time of append and modification--- necessary for write function'''
		try:
			now=time.time()
			now1=time.time()
			atime,mtime=(now,now1)
			st=file_mode[path]
			st.st_atime=atime
			st.st_mtime=mtime
			file_mode[path]=st
			return 0
		except TypeError:
			return self.utime(path,times)
	
	def chown(self,path,uid,gid):#13		
		'''Change ownership of directory/file ---- necessary for write function'''
		global file_mode,dir_mode
		if path in file_mode:
			st=file_mode[path]
			st.st_uid=uid
			st.st_gid=gid
			file_mode[path]=st
		elif path in dir_mode:
			st=dir_mode[path]
			st.st_uid=uid
			st.st_gid=gid
			dir_mode[path]=st


	def utime(self,path,times=None):#14		
		'''set time of append and modification---- necessary for write function'''
		now=time.time()
		now1=time.time()
		atime,mtime=(now,now1)
		st=file_mode[path]
		st.st_atime=atime
		st.st_mtime=mtime
		file_mode[path]=st
		return 0
	
fuse.fuse_python_api=(0,2)

def createfile(name,content):
	'''This function is not used at all'''
	global files,file_content,file_mode
	name='/'+name
	files.append(name)
	file_content[name]=content
	st=MyStat()
	st.st_mode=stat.S_IFREG|0777
	st.st_nlink=1
	st.st_size=len(file_content[name])
	file_mode[name]=st

def main(tbs):
	
	server=FuseFS(version="%prog "+fuse.__version__,usage="Error",tbs=tbs)
	utils.Logger('********tbs',str(tbs[0]))
	utils.Logger('********objs',str(tbs[1]))
	server.parse(errex=1)
	server.main()		#Start the main event loop

def initialize():
	server=FuseFS(version="%prog "+fuse.__version__,usage="Error")
	server.parse(errex=1)
	return server

if __name__=='__main__':
	utils.TruncateFiles()		'''Truncate the log files'''
	tbs,dbobjlist = SQLdir(SQL.listdatabases('localhost','guest','',''))
	utils.Logger('dbobjlist',str(dbobjlist))
	main((tbs,dbobjlist))
