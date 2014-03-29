from fuse import FUSE, Operations, LoggingMixIn		#Get the Fuse class from fuse module
import fuse
import tempfile as tf
import os,stat,urllib,errno,sys
import time
import SQL,utils,MySQLdb

files=[]			# Represent path of the files
dirs={'/':[]}			# Represent directory structure as a key value python dictionary object
file_content={}			# Not necessary
file_mode={}			# The File Modes of each and every path, very important as getattr uses this
dirpath=['/']			# Not necessary
dir_mode={}			# The File Modes of each and every path, very important as getattr uses this
tmpfiles = {}
writefile = {}
prvline = {} 
writehandle = {}
class MyStat(fuse.c_stat):		#The resource statistics as given by fuse.Stat and set to values
	def __init__(self):
		self.st_mode=0
		self.st_nlink=0
		self.st_uid=os.getuid()
		self.st_gid=os.getgid()
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
		if str(db) != 'employees':
			path = folder+str(db)		#/{dbname}	db contains name
		
			dirs[folder].append(path)
			dirpath.append(path)
			dirs[path] = []
			tabs = SQL.DB.listtabs(db,Pdblist)
			SQL.DB.listcreatetables(db,Pdblist)
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
	print "Building Reverse Index Tree"
	counter = 0
	
	for db in Pdblist:
		if 'employees' != str(db) and 'information_schema' not in str(db) and 'mysql' != str(db): 
			counter += db.buildIndex()
	print "Totally indexed ", counter, " records in all databases"
	for db in Pdblist[:]:
		if str(db) == 'employees':
			Pdblist.remove(db)
	return tbs,Pdblist	#return all tab names and dbobjects list and create table as tuple

class FuseFS(LoggingMixIn, Operations):		#Base FUSE CLASS
	test=[]
	
	def __init__(self,tbs):
		#super(FuseFS,self).__init__(version=version,usage=usage)	#Use the superclass init function
		self.tbs,self.dbobjlist = tbs		#tab names and dbobjlist
		utils.Logger('self.dbobjlist',str(self.dbobjlist))
		for t in self.tbs:
			print t,type(t)
			self.mknode(t,stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH,777)	# set all the attribute values of the tables in each database
			self.mknode(t+".schema", stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH, 777)
	
	def  getattr(self,path, fh = None):#1
		'''Get the attributes of a resource'''
		st=MyStat()
		print "TEST ", path
		
		if path in dirpath:
			st=dir_mode[path]
			st.st_mode=stat.S_IFDIR | 0777
			st.st_nlink=2
		elif path in files:
			st=file_mode[path]
			st.st_mode = stat.S_IFREG | 0777
            		st.st_nlink = 1
			
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
			#dbobj.gettabs()			# get the tables in the database
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
		return tt
		#for r in tt:
			#if r[0]!='.' and r[-1]!='~':
		#	yield fuse.Direntry(r)		#yield the data for each and every resource in the current directory

	
	def rmdir(self,path):#3
		p = []
		npath = path+'/'
		
		for l in files:
			if npath in l:
				p.append(l)
		for pp in p:
			self.unlink(pp)
		dirpath.remove(path)
		dir_mode.pop(path)
		dirs.pop(path)
		folder,n=path.rsplit('/',1)
		if folder=='':
			folder='/'
		dirs[folder].remove(path)
		dbobj = SQL.DB.getobject(self.dbobjlist,n)
		if dbobj in self.dbobjlist:
			self.dbobjlist.remove(dbobj)
		dbobj.rmDB(n)
		

	def mkdir(self,path,mode):#5
		folder,name=path.rsplit('/',1)
		if folder=='':
			folder='/'
		else:
			return -errno.EINTR
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
		self.dbobjlist.append(SQL.DB.createdatabase(name))
	
	def rename(self,old,new):#6
		if '.' in new:
			if 'csv' in new.rsplit('.',1)[1]:
				if '.csv' not in old and '.schema' not in old:
					for p in files:
						p = p.rsplit('.',1)
						if p[0] == new.rsplit('.',1)[0] and p[1] == 'schema':
							if old in files:
								files.remove(old)
								files.append(new)
								c=file_content[old]
								file_content.pop(old)
								file_content[new]=c
								c=file_mode[old]
								file_mode.pop(old)
								file_mode[new]=c
								return
				return -errno.EINTR
			elif 'schema' in new.rsplit('.',1)[1]:
				if '.schema' not in old and '.csv' not in old:
					if old in files:
						files.remove(old)
						files.append(new)
						c=file_content[old]
						file_content.pop(old)
						file_content[new]=c
						c=file_mode[old]
						file_mode.pop(old)
						file_mode[new]=c
						if new.rsplit('.',1)[0] + '.csv' not in files:
							SQL.DB.createnewtable(self.dbobjlist,new)
							self.mknode(new.rsplit('.',1)[0],stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH,777)
						return
				elif '~' == new[-1]:
					if old in files:
						files.remove(old)
						files.append(new)
						file_content[new]=''
						c=file_mode[old]
						file_mode.pop(old)
						file_mode[new]=c
					
		elif old in dir_mode:
			print 'DIRECTORY RENAME'
			folder,name = new.rsplit('/',1)
			if folder == '':
				folder = '/'
			dirpath.append(new)
			
			dirs[folder].append(new)
			
			dirs[new]=[]
			db = name
			dbobj = SQL.DB.getobject(self.dbobjlist,db)
			if dbobj in self.dbobjlist:
				self.dbobjlist.remove(dbobj)
			c = dir_mode[old]
			
			dir_mode[new] = c
			self.dbobjlist.append(SQL.DB.createdatabase(db))
			dir_mode.pop(old)
			dirpath.remove(old)
			dirs[folder].remove(old)
			dirs.pop(old)
			return
		return -errno.EINTR
		
	
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
		files.append(path)			#Show schema as table.csv
		st=MyStat()
		st.st_mode=stat.S_IFREG | mode		#Set mode
		st.st_nlink=1
		st.st_ctime=now
		st.st_atime=now
		st.st_mtime=now
		file_mode[path]=st
		file_content[path]=''
		if '.schema' in path:
			SQL.DB.createnewtable(self.dbobjlist,path)
			self.mknode(path.rsplit('.',1)[0],stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH,777)
		print 'Added',path
		print 'MODE',str(st)
		return 0
	
	def mknode(self,path,mode,dev):#7
		print "CREATING FILE ",path
		global files,file_content,file_mode
		now=time.time()
		if '.schema' not in path:
			if path+'.csv' not in files:
				files.append(path+'.csv')		#Show table as table.csv
		else:
			files.append(path)			#Show schema as table.schema
		st=MyStat()
		st.st_mode=stat.S_IFREG | mode		#Set mode
		st.st_nlink=1
		st.st_ctime=now
		st.st_atime=now
		st.st_mtime=now
		if '.schema' not in path:
			file_mode[path+'.csv']=st
		else:
			file_mode[path]=st
		return 0
	
	def flush(self, path):
		return 0
	def release(self, path, flags):
		global files
		for f in files[:]:
			if '#' in f and '~' in f and 'lock' in f:
				self.unlink(f)
	'''def release(self, path, flags):
		if path in tmpfiles:
			
			if path in writefile:
				f = tf.NamedTemporaryFile(mode = 'w+b', prefix = path.rsplit('/',1)[1], bufsize = 1000000000, delete = False)
				f.write('\n'.join(writehandle[path]))
				f.close()
				del writefile[path]
			if path in writehandle:
				f = writehandle[path]
				f.close()
				del writehandle[path]
			tmpfiles[path].unlink(tmpfiles[path].name)
			
			del tmpfiles[path]'''
	def read(self, path, size, offset):#8	#path contains .csv extension
		'''Read a given file'''
		print '~~~~~~~~~~~~~~~~~~~~~~~~READ~~~~~~~~~~~~~~~~~~~',path
		print "SIZE SIZE SIZE ", size, "OFFSET OFFSET OFFSET ", offset
		utils.Logger("SIZE SIZE SIZE " +str( size) +  "OFFSET OFFSET OFFSET "+ str(offset) , "")
		global file_mode
		if ('.schema' not in path and '.csv' not in path) or '~' in path or '#' in path:
			buf = ''
			if path in file_content:
				buf = file_content[path]
				slen = len(buf)
				if offset < slen:
				    buf = buf[offset:offset+size]
				    print buf
				else:
				    buf = ''
			return buf
		try:
			buff = ""
			utils.Logger('read path',str(path))
			print 'OFFSET',offset
			table = path.rsplit('/',1)[1].split('.')[0]		#Get the table name alone from /path/table.csv
			db = path.split('/')[1]					#Get the database the table belongs to
			utils.Logger('db path',str(db))
			dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
			utils.Logger('Got DB Obj',str(dbobj))
			if '.schema' not in path and '.csv' in path and '~' not in path and '#' not in path:
				if path not in tmpfiles:
					f = tmpfiles[path] = tf.NamedTemporaryFile(mode = 'w+b', delete = False, prefix = 'fsql' + table + '.csv' , bufsize = 10000000)
				else:
					if offset == 0:
						f = open(tmpfiles[path].name, "w")
						f.truncate(0)
					else:
						f = open(tmpfiles[path].name, "a")
				print 'writing to tmpfile ~~~~~~~~~~~&&&&&&&' , tmpfiles[path].name
				#buff = dbobj.gettable(table)			# Get the table contents as string
				head = dbobj.gettableheadings(table)		#Get the column headings as string
				pkset = SQL.DB.index[str(dbobj)][table].getRows(offset, size)
				buff = dbobj.gettableoffset(table, pkset)			# Get the table contents as string
				utils.PKSetLogger(table, str(pkset))
				buff = head + buff				# Final string representation
				utils.Logger('Got tab buf',str(buff))
				f.write(buff)
				f.close()
				#return str(buff)
			else:
				print "~~~~~~~~~~~~~~~~~~~~~~~SCHEMA FILE~~~~~~~~~~~~~~~~~~~~~~~~~"
				try:
					buff = dbobj.crtabs[table]
					utils.Logger('Got tab create', str(buff))
				except KeyError:
					print "TABLE DOES NOT EXIST"
					return -errno.ENOENT
			slen = len(buff)
			utils.Logger('Length of '+str(path),str(slen))
			file_mode[path].st_size = slen
			if offset < slen:
			    buf = buff[offset:offset+size]
			    print '~~~~~~~~~~~~~~~~~~~~~~~~~``' + buf
			else:
			    buf = ''
			return str(buf)
		except MySQLdb.Error,e:
			print "MYSQL ERROR ~~~~~~~~~~~~~~", str(e)
			return ""
	
	def fsdetroy(self):
		global tmpfiles
		for f in tmpfiles:
			tmpfiles[f].unlink(tmpfiles[f].name)	

	def unlink(self,path):#9
		global files,file_content,file_mode
		print 'unlink ',path
		if '~' in path and 'lock' in path:
			print "REMOVING ",path
			if path in files:
				files.remove(path)
				if path in file_content:
					file_content.pop(path)
				file_mode.pop(path)
		elif '.schema' in path:
			print "REMOVING ",path
			if path in files:
				files.remove(path)
				if path in file_content:
					file_content.pop(path)
				file_mode.pop(path)
				db = path.split('/')[1]
				tabname = path.rsplit('/',1)[1].rsplit('.',1)[0]
				dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
				dbobj.removeTable(tabname)
				self.unlink1(path.rsplit('.',1)[0] + '.csv' )
		
		elif '.csv' in path:
			print "REMOVING ",path
			if path in files:
				files.remove(path)
				if path in file_content:
					file_content.pop(path)
				file_mode.pop(path)
				db = path.split('/')[1]
				tabname = path.rsplit('/',1)[1].rsplit('.',1)[0]
				dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
				dbobj.removeTable(tabname)
				self.unlink1(path.rsplit('.',1)[0] + '.schema' )
			
	def unlink1(self,path):#9
		global files,file_content,file_mode
		if '.schema' in path:
			print "REMOVING ",path
			if path in files:
				files.remove(path)
				if path in file_content:
					file_content.pop(path)
				file_mode.pop(path)
				db = path.split('/')[1]
				tabname = path.rsplit('/',1)[1].rsplit('.',1)[0]
				dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
				dbobj.removeTable(tabname)
		elif '.csv' in path:
			print "REMOVING ",path
			if path in files:
				files.remove(path)
				if path in file_content:
					file_content.pop(path)
				file_mode.pop(path)
				db = path.split('/')[1]
				tabname = path.rsplit('/',1)[1].rsplit('.',1)[0]
				dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
				dbobj.removeTable(tabname)

	
	def write(self,path,data,offset):#10
		
		global file_content,files,file_mode
		print '~~~~~~~~~~~~~~~~~~~Write~~~~~~~~~~~~~~~~~~~',path
		if '.schema' in path:
			table = path.rsplit('/',1)[1].split('.')[0]		#Get the table name alone from /path/table.csv
			db = path.split('/')[1]					#Get the database the table belongs to
			utils.Logger('db path',str(db))
			dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
			dbobj.crtabs[table] = data 
			if dbobj.createTable(table, 1) != -1:
				return len(data)
		elif '.csv' in path and '~' not in path and '#' not in path:
			if path not in writefile:
				writefile[path] = []
			if data[-1] != '\n':
				print 'NOT COMPLETE RECORD'
				data, prvline[path] = data.rsplit('\n',1)
			else:
				prvline[path] = ''
			if offset == 0:
				if path not in writehandle:
					f = open(tmpfiles[path].name,'r')
					writehandle[path] = f
			f = writehandle[path]
			data = data.split('\n')
			for d in data:
				if d != f.readline():
					writefile[path].append(d)
			return len(data)
			'''if data[-1] != '\n':
				print 'NOT COMPLETE RECORD'
			utils.Logger('read path',str(path))
			print 'OFFSET',offset
			table = path.rsplit('/',1)[1].split('.')[0]		#Get the table name alone from /path/table.csv
			db = path.split('/')[1]					#Get the database the table belongs to
			utils.Logger('db path',str(db))
			dbobj = SQL.DB.getobject(self.dbobjlist,db)	#get object corresponding to dbname
			utils.Logger('Got DB Obj',str(dbobj))
			pkset = SQL.DB.index[str(dbobj)][table].getRows(offset, len(data))
			print len(pkset)
			print pkset
			print '~' * 40
			print data
			print '~' * 40
			return len(data)'''
		else:
			file_content[path] = data
			return len(data)
			'''
			if offset == 0:
				data = data.split('\n')[1:]
				data = '\n'.join(data)
			utils.Logger('Write to file',str(path)+'>>>>>'+str(data))
			#print 'DATA',data
			#print 'OFFSET', offset
			#lastline = data.split('\n')[-1]
			#r = lastline.split(utils.delim)
			db = path.split('/')[1]			#Get the database the table belongs to
			table = path.rsplit('/',1)[1].split('.')[0]	#get the table name alone from /path/..../table.csv
			dbobj = SQL.DB.getobject(self.dbobjlist,db)
			if not r[-1] or len(r) != dbobj.getRecsize(table):
				print 'Rec Size low', len(r)
				lines = data.split('\n')[:-1]
				data = '\n'.join(lines)
				print "DATA LEN", len(data)
				print "HALF REC", r
				print 'Bottom Rec',lines[-1]
				dbobj.setlastRec(table, lastline)
		
			dbobj.settabcontent(table, data, offset)		#call the modificaiton function with the table name and data
		
			return len(data)			# return positive len(data) to show success
			st=file_mode[path]
			c=file_content[path]
			newc=data
			file_content[path]=newc
			st.st_size=len(file_content[path])
			file_mode[path]=st
			return len(data)
			'''
	
	def truncate(self, path, length):#11
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
	
#fuse.fuse_python_api=(0,2)

def main(tbs):
	fu = FUSE(FuseFS(tbs), sys.argv[1], foreground=True)
	'''server=FuseFS(version="%prog "+fuse.__version__,usage="Error",tbs=tbs)
	utils.Logger('********tbs',str(tbs[0]))
	utils.Logger('********objs',str(tbs[1]))
	server.parse(errex=0)
	server.main()		#Start the main event loop'''


if __name__=='__main__':
	utils.TruncateFiles()		#'''Truncate the log files'''
	tbs,dbobjlist = SQLdir(SQL.listdatabases('localhost','root','',''))
	utils.Logger('dbobjlist',str(dbobjlist))
	main((tbs,dbobjlist))
	#utils.testIndex(dbobjlist)
