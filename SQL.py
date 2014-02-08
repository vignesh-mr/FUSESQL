import MySQLdb
import utils
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
	def __init__(self, db):
		self.db = db		#set dbname - string name
		self.content = {}	#string representation of table data
		self.headings = {}	#columns headings
		self.cset = {}		#set of rows in a table
		self.keys = {}		#Primary key of the table
		self.nkeys = {}		#Number of columns in a primary key
		self.types = {}		#Type of each and every column
	
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
		if tabname in self.content:
			utils.Logger('Get Table Size',str(self)+'.'+str(tabname))
			utils.Logger('Table Size',str(len(self.content[tabname])))
			return len(self.content[tabname])
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
			db.commit()
		except:
			db.rollback()
			cursor.close()
			db.close()
			raise Exception('DELETE SQL','COMMIT ERROR')
		if tabname in self.content:
			del self.content[tabname]
			self.content[tabname] = self.gettable(tabname)		
	
	def settabcontent(self, tabname, data):
		'''Base function for modifications 
		based on the data and the previous data in table - tabname, perform either insert, update or delete'''
		
		old = self.cset[tabname]		#Tab data as a set
		keys = self.keys[tabname]		#key columns in table
		new = set(data.split('\n')[1:])		# the current data buffer value as a set
		cur = new - old				# corresponding new values in the current data
		print "CUR",cur
		print len(cur)
		delete = old - new			# Old values that don't occur in the new data
		old = self.content[tabname].split('\n')
		oo = []
		print 'ODL B4 SPLIT',old
		for c in old:
			if c:
				c = c.split(',')
				oo.append(c)
		old = oo				# old is a [[]] list of lists of old data value
		cold = []
		for c in cur:
			if c:
				c = c.split(',')
				cold.append(c)			#cold is a [[]] list of lists of values to be modified
		print 'TO DELETE',delete
		for d in delete:
			r = d.split(',')
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
		print 'ODL',old
		print 'CUR',cur
		for c in cur:
			r = c.split(',')
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
			db.commit()
		except:
			db.rollback()
			cursor.close()
			db.close()
			raise Exception('INSERT SQL','COMMIT ERROR')
		print self.content.keys()
		if tabname in self.content:
			del self.content[tabname]
			self.content[tabname] = self.gettable(tabname)
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
			db.commit()
		except:
			db.rollback()
			cursor.close()
			db.close()
			raise Exception('UPDATE SQL','COMMIT ERROR')		
		if tabname in self.content:
			del self.content[tabname]
			self.content[tabname] = self.gettable(tabname)
		cursor.close()
		db.close()
		
	def setvaltype(self, tabname, v, colnum):
		'''Based on the type of data, construct the string representation of with or without quotes'''
		types = self.types[tabname]
		if 'varchar' in types[colnum] or 'date' in types[colnum]:
			return "'" + v + "' "
		else:
			return  v
	def setcontenttype(self, tabname, k, v, colnum):
		'''Based on the type of data, construct the string representation of key and value with or without quotes'''
		types = self.types[tabname]
		if 'varchar' in types[colnum] or 'date' in types[colnum]:
			return k + "='" + v + "' "
		else:
			return k + '=' + v
	def gettableheadings(self, table):
		'''Get the column heading from the database'''
		if table in self.headings:
			return ','.join(self.headings[table])+'\n'
		else:
			db = DBConnect.getdatabase(d = str(self))
			cursor = db.cursor()
			cursor.execute('desc ' + table)
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
			utils.SQLLogger('SELECT', 'select * from '+tabname)
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
	utils.SQLLogger('SHOW','show databases')
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
		utils.SQLLoggger('SHOW','show tables')
		tabs = cur.fetchall()
		
		cur.close()
		db.close()
		dbname.settabs(tabs)
		L.append(dbname)
		print d,"TABLS",tabs
	return L
