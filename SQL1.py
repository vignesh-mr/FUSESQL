import MySQLdb
import json,string
import utils1 as utils

NROWS = 1000

class DBConnect:
	'''Class for getting a connection to MySQL instance'''
	def __init__(self,h,u,p,d):
		DBConnect.h = h
		DBConnect.u = u
		DBConnect.p = p
		DBConnect.d = d
	
	@staticmethod
	def getdatabase(h = None,u = None,p = None,d = None):
		'''based on d value - dbname, get the connection'''
		if d :
			return MySQLdb.connect(host=DBConnect.h,user=DBConnect.u,passwd=DBConnect.p,db=d)
		else:
			return MySQLdb.connect(host=DBConnect.h,user=DBConnect.u,passwd=DBConnect.p,db=DBConnect.d)

class DB:
	index = {}
	def __init__(self, db):
		self.lastRec = {}
		self.lrindex = {}
		self.db = db		#set dbname - string name
		self.content = {}	#string representation of table data
		self.size = {}
		self.headings = {}	#columns headings
		self.cset = {}		#set of rows in a table
		self.keys = {}		#Primary key of the table
		self.nkeys = {}		#Number of columns in a primary key
		self.types = {}		#Type of each and every column
		self.crtabs = {}
		self.tabs = []
		self.tabfiles = {}	#{name:startoffset}
	
	def buildIndexTab(self, tab):
		tindex = {}
		self.gettableheadings(tab)
		tindex[tab],counter = utils.buildIndex(tab, self.gettableIndex(tab), self.keys[tab])
		DB.index[str(self)].update(tindex)
		print tindex[tab].printOT()
		
	def buildIndex(self):
		count = 0
		tindex = {}
		print "INDEX builinding for", str(self)
		for tab in self.tabs:
			self.gettableheadings(tab)
			tindex[tab],counter = utils.buildIndex(tab, self.gettableIndex(tab), self.keys[tab])
			count += counter
		print "Indexed ", count, "records in database ", str(self)
		DB.index[str(self)] = tindex
		return count
	
	@staticmethod
	def createnewtable(dbobjlist, path):
		db,tabname = path.rsplit('/',1)
		db = db.split('/')[1]
		dbobj = DB.getobject(dbobjlist,db)	#get object corresponding to dbname
		dbobj.createTable(tabname.rsplit('.',1)[0])
	
	def getKeys(self, tabname):
		return self.keys.get(tabname)
	
		
	
	def createTable(self,tabname, flag = 0):
		if flag == 0:
			print "CREATE NEW TABLE"
			self.removeTable(tabname)
			db = DBConnect.getdatabase(d = str(self))
			cursor = db.cursor()
			sql = ' create table ' + str(self) + "." + tabname + ' (id int)'
			print sql
			cursor.execute(sql)
			db.commit()
			utils.SQLLogger('CREATE', sql)
			self.tabs.append(tabname)
			cursor.close()
			db.close()
			DB.getcreatetable(self, [tabname])
		else:
			self.removeTable(tabname)
			r = self.crtabs[tabname]
			c = ""
			for ch in r:
				if ch == '\t' or ch == '\n' or ch == "'":
					c += " "
				else:
					c += ch
			r = c
			sql = ' create table ' + str(self) + "." + tabname + r
			db = DBConnect.getdatabase(d = str(self))
			cursor = db.cursor()
			print sql
			try:
				cursor.execute(sql)
				db.commit()
			except MySQLdb.Error, e:
				print "MYSQL ERROR ~~~~~~~~~~~~~~~~~~~~~~~~", e
				cursor.close()
				db.close()
				return -1
			utils.SQLLogger('CREATE', sql)
			cursor.close()
			db.close()
			DB.getcreatetable(self, [tabname])
	
	def removeTable(self,tabname):
		print "DROP TABLE"
		db = DBConnect.getdatabase(d = str(self))
		cursor = db.cursor()
		sql = 'drop table if exists '+ str(self) + '.' +tabname 
		print sql
		cursor.execute(sql)
		db.commit()
		utils.SQLLogger('DROP TABLE', sql)
		cursor.close()
		db.close()
	
	def setlastRec(self, tabname, l):
		self.lastRec = l
	
	def settabs(self, tabtup):
		self.tabs = []
		for tabs in tabtup:		#Given a set of tablenames, set the instance property
			self.tabs.append(tabs[0])
		
	@staticmethod
	def listtabs(db, L):
		'''Given the db and list of dbobjects -- L, list the tables in the given db'''
		for d in L:
			if db == str(d):
				return d.tabs	#return list of tab names
	
	@staticmethod
	def listcreatetables(db, L):
		'''Given the db and list of dbobjects -- L, list the create tables in the given db'''
		for d in L:
			if db == str(d):
				DB.getcreatetable(d,d.tabs)
				return	
	
	def rmDB(self,name):
		if ' ' not in name:
			db = DBConnect.getdatabase()
			cursor = db.cursor()
			sql = 'drop database if exists '+ name
			print sql
			cursor.execute(sql)
			db.commit()
			utils.SQLLogger('DROP', sql)
			cursor.close()
			db.close()
			
	
	def alterTable(self, add, remove, tabname, flag = 0):
		head = self.headings[tabname]
		head = [h.lower() for h in head]
		head = [h.split('(')[0] if '(' in h else h for h in head]
		print head
		change = []
		for a in add:
			na, da = a.split(' ',1)
			na = na.replace("'","")
			print na + '~'
			print da + '~'
			sql = ''
			if na in head:
				print 'ALREADY THERE'
				print 'only modify column'
				change.append(na)
				sql = 'alter table ' + str(self) + '.' + tabname + " modify " + na + " " + da
				print sql
				
			else:
				print 'NEW COLUMN'
				sql = 'alter table ' + str(self) + '.' + tabname + " add " + na + " " + da
				print sql
			if sql:
				db = DBConnect.getdatabase(d = str(self))
				cursor = db.cursor()
				print sql
				try:
					cursor.execute(sql)
					db.commit()
				except MySQLdb.Error, e:
					print "MYSQL ERROR ~~~~~~~~~~~~~~~~~~~~~~~~", e
					cursor.close()
					db.close()
					return -1
				utils.SQLLogger('ALTER', sql)
				cursor.close()
				db.close()
			
		for r in remove:
			nr, dr = r.split(' ',1)
			nr = nr.replace("'","")
			sql = ''
			if nr in head and nr in change:
				print 'ALREADY THERE HANDLED IN add CASE'
			else:
				print 'NEW COLUMN'
				sql = 'alter table ' + str(self) + '.' + tabname + " drop " + nr 
				print sql
			if sql:
				db = DBConnect.getdatabase(d = str(self))
				cursor = db.cursor()
				print sql
				try:
					cursor.execute(sql)
					db.commit()
				except MySQLdb.Error, e:
					print "MYSQL ERROR ~~~~~~~~~~~~~~~~~~~~~~~~", e
					cursor.close()
					db.close()
					return -1
				utils.SQLLogger('ALTER', sql)
				cursor.close()
				db.close()
		del self.headings[tabname]
		self.gettableheadings(tabname)
		self.buildIndexTab(tabname)
		DB.getcreatetable(self, [tabname])
		return 0
			
	@staticmethod
	def createdatabase(name):
		if ' ' not in name:
			db = DBConnect.getdatabase()
			cursor = db.cursor()
			sql = 'drop database if exists '+ name
			print sql
			cursor.execute(sql)
			db.commit()
			utils.SQLLogger('DROP', sql)
			cursor = db.cursor()
			sql = 'create database '+ name
			print sql
			cursor.execute(sql)
			db.commit()
			utils.SQLLogger('CREATE', sql)
			cursor.close()
			db.close()
		return DB(name)
	
	@staticmethod
	def getcreatetable(d, tabs):
		db = DBConnect.getdatabase(d= str(d))
		crtabs = {}
		for tab in tabs:
			cursor = db.cursor()
			cursor.execute('show create table ' + str(d) + "." + str(tab))
			r = cursor.fetchall()
			r = r[0][1]
			r = r.split('(',1)[1]
			#r = r.rsplit(')', 1)[0]
			c = ""
			for ch in r:
				if ch == '`':
					c += "'"
				else:
					c += ch
			r = c
			r = r.rsplit(')',1)[0]
			r = '(' + r + ')\n'
			'''
			ll = []
			for c in r:
				ll.append(c.strip())
			r = ll
			js = {}
			print str(d),tab
			print r
			for t in r:
				if 'CONSTRAINT ' in t or 'UNIQUE KEY' in t or 'PRIMARY KEY' in t:
					t = t.split(' ', 2)
					h = []
					h.append(t[0] + ' ' + t[1])
					for e in t[2:]:
						h.append(e)
					t = h
					print t
				else:
					t = t.split(' ',1)
				print "TEST",t
			#	if str(d) == 'Project':
			#		raw_input()
				if t[0] != '':
					if ',' == t[1][-1]:
						js[t[0]] = t[1][:-1] 
					else:
						js[t[0]] = t[1]
			print js
			#if str(d) == 'Project':
			#		raw_input()
			r = json.dumps(js, indent = 0)
			'''
			print r
			#if str(d) == 'Project':
			#		raw_input()
			crtabs[str(tab)] = str(r)
			
			cursor.close()
		
		db.close()
		d.crtabs = crtabs
		return				
	
	@staticmethod
	def tabpath(db, tabname):
		return '/'+str(db)+'/'+str(tabname)
	
	def __str__(self):
		'''Return string reprensentation of object ---- here only name'''
		return str(self.db)
	
	@staticmethod
	def listdatabases(L):
		'''Given a list of db objects --- L, return a list of their string names'''
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
		'''Get the character size in bytes for the table'''
		tabname = str(tabname).rsplit('/',1)[1].split('.')[0]
		if tabname in self.size:	#self.content:
			utils.Logger('Get Table Size',str(self)+'.'+str(tabname))
			utils.Logger('Table Size',str(self.size[tabname]))
			return self.size[tabname]
		return 0
		
	def deletedb(self, tabname, r):
		'''Delete the given row r from table tabname'''
		keys = self.keys[tabname]
		head = self.headings[tabname]
		dele = 'delete from ' + str(self) + '.' + tabname +' where '
		where = []
		
		for i in range(len(keys)):
			if keys[i] != 'NU&&':
				where.append(self.setcontenttype(tabname, keys[i], r[i], i))
			else:
				where.append(self.setcontenttype(tabname, head[i], r[i], i))
		where = ' and '.join(where)
		sql = dele + where
		print sql
		db = DBConnect.getdatabase(d = str(self))
		cursor = db.cursor()
		try:
			cursor.execute(sql)
			utils.SQLLogger('DELETE', sql)
			#db.commit()
		except:
			db.rollback()
			cursor.close()
			db.close()
			raise Exception('DELETE SQL','COMMIT ERROR')
		#if tabname in self.content:
		#	del self.content[tabname]
		#	self.content[tabname] = self.gettable(tabname)		
	
	def settabcontent(self, tabname, data, offset):
		'''Base function for modifications 
		based on the data and the previous data in table - tabname, perform either insert, update or delete'''
		

		
		old = self.gettable(tabname)	#self.content[tabname].split('\n')		#Tab data as a set
		
		keys = self.keys[tabname]		#key columns in table
		new = set(data.split('\n'))		# the current data buffer value as a set
		print new
		#if offset != 0:
		#	old = old[self.lrindex[tabname] : self.lrindex[tabname] + len(new)]
		#	self.lrindex[tabname] += len(new) + 1
		#else:
		#	self.lrindex[tabname] = 0
		#print 'Top Rec', old[0]
		olds = set(old)
		cur = new - olds				# corresponding new values in the current data
		print len(olds)
		print len(new)
		#print "CUR",cur
		
		print len(cur)
		delete = olds - new			# Old values that don't occur in the new data
		#old = self.content[tabname].split('\n')	#old has implicit order as displayed
		oo = []
		#print 'ODL B4 SPLIT',old
		for c in old:
			if c:
				c = c.split(utils.delim)
				oo.append(c)
		old = oo				# old is a [[]] list of lists of old data value still has implicit order
		cold = []
		for c in cur:
			if c:
				c = c.split(utils.delim)
				cold.append(c)			#cold is a [[]] list of lists of values to be modified
		#print 'TO DELETE',delete
		#self.checkPKmodification(tabname, delete, cur, cold, old, new)
		for d in delete:
			r = d.split(utils.delim)
			for i in range(len(r)):
				flag = 0
				for p in cold:		
					print 'cur data', p		# A list is an ordered data type
					print 'TAB KEYS',keys		# keys has column heading if a primary key otherwise a value NU&&
					if r[i] == p[i] and keys[i] != 'NU&&':
						flag += 1		#check if all primary keys match
				print 'FLAG VAL DELETE',flag
				if flag == self.nkeys[tabname]:
					print 'row for update'		# if all PKs match then it is an update 
					break
				else:
					self.deletedb(tabname, r)	#delete the row from table
					break
		#print 'ODL',old
		print 'CUR',cur
		for c in cur:
			r = c.split(utils.delim)
			for i in range(len(r)):
				flag = 0
				for p in old:
					
					print 'old data', p			# A list is an ordered data type
					print 'TAB KEYS',keys			# keys has column heading if a primary key otherwise a value NU&&
					if r[i] == p[i] and keys[i] != 'NU&&':					
						flag += 1		#check if all primary keys match
					
				if flag == self.nkeys[tabname]:
					print 'going to update'		# if all PKs match but the data tuple is different from the old one
					self.updatedb(tabname, r)	# then some other column is updated in the data tuple
					break
				else:
					print 'going to insert'
					self.insertdb(tabname, r)	# Else it is a new data tuple to be added
					break
	
	'''
	def checkPKmodificatoin(self, delete, cur, cold, old, new):
		common = delete & cur
		delete = delete - common
		cur = cur - common
		for r in common:
			self.commonupdatedb(tabname, r)
	'''
	
	def getRecsize(self, tabname):
		print 'get rec size'
		return len(self.keys[tabname])
	
	def insertdb(self, tabname, r):
		'''Insert a new row r into table tabname'''
		print 'ROW', r
		keys = self.keys[tabname]
		head = self.headings[tabname]
		db = DBConnect.getdatabase(d = str(self))
		cursor = db.cursor()
		insert = 'insert into '+ str(self) + '.' + tabname+' ('
		theads = []
		vals = []
		for i in range(len(keys)):
			vals.append(self.setvaltype(tabname, r[i], i))
			if keys[i] != 'NU&&':
				theads.append(keys[i])
			else:
				theads.append(head[i])
		theads = ','.join(theads) + ') ' 
		vals = 'values (' + ','.join(vals) + ')'
		sql = insert + theads + vals
		print sql
		try:
			cursor.execute(sql)
			utils.SQLLogger('INSERT', sql)
			#db.commit()
		except:
			db.rollback()
			cursor.close()
			db.close()
			raise Exception('INSERT SQL','COMMIT ERROR')
		#print self.content.keys()
		#if tabname in self.content:
		#	del self.content[tabname]
		#	self.content[tabname] = self.gettable(tabname)
			
		#	print self.content[tabname]
		cursor.close()
		db.close()
		
	def updatedb(self, tabname, r):
		'''Update the row r with the value r, assuming PKs same in the table tabname'''
		keys = self.keys[tabname]
		db = DBConnect.getdatabase(d = str(self))
		cursor = db.cursor()
		sql = 'update '+ str(self) + '.' + tabname +' set '
		cols = []
		where = []
		for i in range(len(keys)):
			if keys[i] != 'NU&&':	#Assuming primary keys remain the same
				where.append(self.setcontenttype(tabname, keys[i], r[i], i))
			else:
				cols.append(self.setcontenttype(tabname, self.headings[tabname][i], r[i], i))
		where = ' where ' + ' and '.join(where)
		cols = ','.join(cols)
		sql = sql + cols + where
		print sql
		cursor.close()
		cursor = db.cursor()
		try:
			cursor.execute(sql)
			utils.SQLLogger('UPDATE', sql)
			#db.commit()
		except:
			db.rollback()
			cursor.close()
			db.close()
			raise Exception('UPDATE SQL','COMMIT ERROR')		
		#if tabname in self.content:
		#	del self.content[tabname]
		#	self.content[tabname] = self.gettable(tabname)
		cursor.close()
		db.close()
		
	def setvaltype(self, tabname, v, colnum):
		'''Based on the type of data, construct the string representation of with or without quotes'''
		types = self.types[tabname]
		j = colnum
		if 'char' in types[j] or "'" in types[j] or 'varchar' in types[j] or 'date' in types[j]:
			return "'" + v + "' "
		else:
			return  v
	def setcontenttype(self, tabname, k, v, colnum):
		'''Based on the type of data, construct the string representation of key and value with or without quotes'''
		types = self.types[tabname]
		j = colnum
		if 'char' in types[j] or "'" in types[j] or 'varchar' in types[j] or 'date' in types[j]:
			return k + "='" + v + "' "
		else:
			return k + '=' + v
	def gettableheadings(self, table):
		'''Get the column heading from the database'''
		if table in self.headings:
			return utils.delim.join(self.headings[table])+'\n'
		else:
			db = DBConnect.getdatabase(d = str(self))
			cursor = db.cursor()
			cursor.execute('desc ' + str(self) + '.' + table)
			utils.SQLLogger('DESCRIBE', 'desc ' + table)
			head = []
			keys = []
			nkeys = 0
			types = []
			for row in cursor.fetchall():
				types.append(row[1]) 
				if 'PRI' in row[3]:
					keys.append(str(row[0]))		#implicit order is maintained
					nkeys += 1
					head.append(str(row[0])+'(PRIMARY KEY)')
				else:
					keys.append('NU&&')		#all other columns are represented as NU&&
					head.append(row[0])
			self.headings[table] = head
			self.keys[table] = keys
			self.nkeys[table] = nkeys
			self.types[table] = types
			return utils.delim.join(self.headings[table])+'\n'
			
	def gettable(self, tabname):
		'''Get the tale from the database'''
		utils.Logger('Get the table from database',str(tabname))
		if tabname in self.tabs:
			#try:
			utils.Logger('Connect to database',str(self))
			db = DBConnect.getdatabase(d = str(self))
			utils.Logger('Get cursor',str(db))
			cursor  = db.cursor()
			utils.Logger('Got Cursor',str(db))
			utils.SQLLogger('SELECT', 'select * from '+ str(self) + '.' +tabname)
			utils.Logger('SELECT', 'select * from '+ str(self) + '.' +tabname)
			cursor.execute('select * from '+ str(self) + '.' + tabname)
			
			utils.Logger('SQL Execute',str(cursor))
			buf = ""
			c = cursor.fetchone()
			while c:
				utils.Logger('Each row',str(c))
				c = ('NULL' if not x else x for x in c)
				utils.Logger('Each row type',str(type(c)))
				c = [str(v) for v in c]
				utils.Logger('Each row list',str(c))
				line = utils.delim.join(c)
				utils.Logger('Each row csv',str(line))
				buf += line + '\n'
				c = cursor.fetchone()
			#cset = buf.split('\n')
			#cset = set(cset)
			#self.cset[tabname] = cset
			self.size[tabname] = len(str(buf))
			utils.Logger('Full Table csv',str(buf))
			return buf
	def gettableoffset(self, tabname, pkset):
		'''Get the tale from the database'''
		utils.Logger('Get the table from database',str(tabname))
		if tabname in self.tabs:
			#try:
			utils.Logger('Connect to database',str(self))
			db = DBConnect.getdatabase(d = str(self))
			utils.Logger('Get cursor',str(db))
			print "~!@#~!@#"
			print pkset
			print "~!@#~!@#"
			if pkset:
				sql = 'select * from '+ str(self) + '.' + tabname + ' where '
			
				if pkset:
					pk = pkset[0]
					length = len(pk)
				else:
					length = 0
				pks = []
			
				for i in range(length):
					pks.append([])
				for pk in pkset:
					for i in range(length):
						pks[i].append(pk[i])
				i = 0
				head = self.headings[tabname]
				print head
				types = self.types[tabname]
				print types
				where = []
				for j in range(len(head)):
					if '(PRIMARY KEY)' in head[j]:
						H = head[j][:string.find(head[j],'(PRIMARY KEY)')]
						print H, types[j]
						if 'char' in types[j] or "'" in types[j] or 'varchar' in types[j] or 'date' in types[j]:
					 		where.append(H + ' in (' + ','.join(["'" + v + "'" for v in pks[i]]) + ") ")
					 		i += 1
					 	else:
					 		where.append(H + ' in (' + ','.join(pks[i]) + ") ")
					 		i += 1		
				where = ' and '.join(where)
				tsql = sql + where
				utils.SQLLogger('SELECT', tsql)
				utils.Logger('SELECT', tsql)
				print tsql
				cursor  = db.cursor()
				utils.Logger('Got Cursor',str(db))
				try:
					cursor.execute('select * from '+ str(self) + '.' + tabname)
				except MySQLdb.Error, e:
					print " Error IN SQL ",e
				utils.Logger('SQL Execute',str(cursor))
				buf = ""
		
				c = cursor.fetchone()
				while c:
					utils.Logger('Each row',str(c))
					c = ('NULL' if not x else x for x in c)
					utils.Logger('Each row type',str(type(c)))
					c = [str(v) for v in c]
					utils.Logger('Each row list',str(c))
					line = utils.delim.join(c)
					utils.Logger('Each row csv',str(line))
					buf += line + '\n'
					c = cursor.fetchone()
					#cset = buf.split('\n')
				#cset = set(cset)
				#self.cset[tabname] = cset
				self.size[tabname] = len(str(buf))
				utils.Logger('Full Table csv',str(buf))
				return buf
			return ''

	def gettableIndex(self, tabname):
		'''Get the tale from the database'''
		utils.Logger('Get the table from database',str(tabname))
		if tabname in self.tabs:
			#try:
			utils.Logger('Connect to database',str(self))
			db = DBConnect.getdatabase(d = str(self))
			utils.Logger('Get cursor',str(db))
			cursor  = db.cursor()
			utils.Logger('Got Cursor',str(db))
			utils.SQLLogger('SELECT', 'select * from '+ str(self) + '.' +tabname)
			utils.Logger('SELECT', 'select * from '+ str(self) + '.' +tabname)
			cursor.execute('select * from '+ str(self) + '.' + tabname)
			
			utils.Logger('SQL Execute',str(cursor))
			
			c = cursor.fetchone()
			while c:
				buf = ""
				utils.Logger('Each row',str(c))
				c = ('NULL' if not x else x for x in c)
				utils.Logger('Each row type',str(type(c)))
				c = [str(v) for v in c]
				utils.Logger('Each row list',str(c))
				line = utils.delim.join(c)
				utils.Logger('Each row csv',str(line))
				buf += line + '\n'
				#print buf
				c = cursor.fetchone()
				yield buf
	
def listdatabases(h,u,p,d):
	connection = DBConnect(h,u,p,d)
	db = DBConnect.getdatabase(h,u,p,d)
	cur = db.cursor()
	cur.execute('show databases')
	utils.SQLLogger('SHOW','show databases')
	baselist = []
	ldb = cur.fetchall()
	cur.close()
	db.close()
	L = []
	print ldb
	for r in ldb:
		baselist.append(r[0])
		d = r[0]	#db name
		dbobj = DB(d)

		db = DBConnect.getdatabase(h,u,p,d)
		cur = db.cursor()
		cur.execute('show tables;')
		utils.SQLLogger('SHOW','show tables')
		tabs = cur.fetchall()
		print '~',tabs,'~'
		if tabs:
			print type(tabs),type(tabs[0])
		#raw_input()
		cur.close()
		db.close()
		dbobj.settabs(tabs)
		L.append(dbobj)
		print d,"TABLS",tabs
	return L
