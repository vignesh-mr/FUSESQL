import MySQLdb
import utils
class DBConnect:
	def __init__(self,h,u,p,d):
		DBConnect.h = h
		DBConnect.u = u
		DBConnect.p = p
		DBConnect.d = d
	@staticmethod
	def getdatabase(h = None,u = None,p = None,d = None):
		if d :
			return MySQLdb.connect(host=DBConnect.h,user=DBConnect.u,passwd=DBConnect.p,db=d)
		else:
			return MySQLdb.connect(host=DBConnect.h,user=DBConnect.u,passwd=DBConnect.p,db=DBConnect.d)

class DB:
	def __init__(self, db):
		self.db = db	#string name
	def settabs(self, tabtup):
		self.tabs = []
		for tabs in tabtup:
			self.tabs.append(tabs[0])
		self.content = {}
		self.headings = {}
		self.cset = {}
	@staticmethod
	def listtabs(db, L):
		for d in L:
			if db == str(d):
				return d.tabs	#return list of tab names
	@staticmethod
	def tabpath(db, tabname):
		return '/'+str(db)+'/'+str(tabname)
	def __str__(self):
		return str(self.db)
	@staticmethod
	def listdatabases(L):
		ldb = []
		for db in L:
			ldb.append(str(db))
		return ldb
	@staticmethod
	def getobject(L, dbname):
		'''Get the object for the corresponding db name'''
		utils.Logger('Get Db object',str(dbname))
		for d in L:
			if dbname == str(d):
				return d
	def gettabs(self):
		'''Get the content of tables in the file'''
		utils.Logger('Get content of',str(self.tabs))
		for table in self.tabs:
			if table not in self.content:
				self.content[table] = self.gettable(table)
				utils.Logger('set table content',str(table))
	def gettabsize(self, tabname):
		tabname = str(tabname).rsplit('/',1)[1].split('.')[0]
		if tabname in self.content:
			utils.Logger('Get Table Size',str(self)+'.'+str(tabname))
			utils.Logger('Table Size',str(len(self.content[tabname])))
			return len(self.content[tabname])
		return 0
	def settabcontent(self, tabname, data):
		old = self.cset[tabname]
		new = set(data.split('\n'))
		cur = new - old
		print "CUR",cur
		print len(cur)
	def gettableheadings(self,table):
		if table in self.headings:
			return ','.join(self.headings[table])+'\n'
		else:
			db = DBConnect.getdatabase(d = str(self))
			cursor = db.cursor()
			cursor.execute('desc '+table)
			head = []
			for row in cursor.fetchall():
				if 'PRI' in row[3]:
					head.append(str(row[0])+'(PRIMARY KEY)')
				else:
					head.append(row[0])
			self.headings[table] = head
			return ','.join(self.headings[table])+'\n'
	def gettable(self, tabname):
		'''Get the tale from the database'''
		utils.Logger('Get the table from database',str(tabname))
		if tabname in self.content:
			return self.content[tabname]
		if tabname in self.tabs:
			#try:
			utils.Logger('Connect to database',str(self))
			db = DBConnect.getdatabase(d = str(self))
			utils.Logger('Get cursor',str(db))
			cursor  = db.cursor()
			utils.Logger('Got Cursor',str(db))
			cursor.execute('select * from '+tabname)
			utils.Logger('SQL Execute',str(cursor))
			buf = ""
			for c in cursor.fetchall():
				utils.Logger('Each row',str(c))
				utils.Logger('Each row type',str(type(c)))
				c = [str(v) for v in c]
				utils.Logger('Each row list',str(c))
				line = ','.join(c)
				utils.Logger('Each row csv',str(line))
				buf += line + '\n'
			cset = buf.split('\n')
			cset = set(cset)
			self.cset[tabname] = cset
			utils.Logger('Full Table csv',str(buf))
			return buf
			#except: 
			#	utils.Logger('Exception','Table Read Error')
def listdatabases(h,u,p,d):
	connection = DBConnect(h,u,p,d)
	db = DBConnect.getdatabase(h,u,p,d)
	cur = db.cursor()
	cur.execute('show databases')
	baselist = []
	ldb = cur.fetchall()
	cur.close()
	db.close()
	L = []
	for r in ldb:
		baselist.append(r[0])
		d = r[0]	#db name
		dbname = DB(d)

		db = DBConnect.getdatabase(h,u,p,d)
		cur = db.cursor()
		cur.execute('show tables;')
		tabs = cur.fetchall()
		
		cur.close()
		db.close()
		dbname.settabs(tabs)
		L.append(dbname)
		print d,"TABLS",tabs
	return L
