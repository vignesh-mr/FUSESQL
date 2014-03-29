import SQL
import sys
delim = '~'
RB = 512
OB = 512 * 32
part = 'PaRt'

class rowIndex:
	ID = 0
	def __init__(self, start):
		self.startoffset = start
		self.size = 0
		self.max = RB
		self.nrows = 0
		self.next = None
		self.id = rowIndex.ID + 1
		rowIndex.ID += 1
		self.pkset = self.recsize = []
		
	def insert(self, row, pks):		#pks [] of indexes which make up primary key
		size = len(row)
		if self.nrows < SQL.NROWS:
			pk = []
			pksize = [pk, size]
			cols = row.split(delim)
			for i in pks:
				pk.append(cols[i])
			self.size += size
			self.recsize.append(pksize) 
			self.nrows += 1
			return 1
		else:
			##print 'FINAL SIZE ', self.size
			return 0
	def delete(self, row, pks):
		pk = []
		cols = row.split(delim)
		for i in pks:
			pk.append(cols[i])
		if pk in self.pkset:
			#print "REMOVED"
			self.pkset.remove(pk)
			size = len(row)
			self.size -= size
			return 1
		else:
			return 0
	def present(self, offset):
		if offset >= self.startoffset and offset < self.startoffset + self.size:
			#print offset,"PRESENT IN ROW BLOCK ", self.id
			#raw_input(str(self.pkset))
			return True
		return False	
	def getRowsPart(self):
		pk = []
		for p in self.pkset:
			pk.append(p[0])
		return pk
	def getRows(self, start, size):
		f = self
		pk = []
		if f and start >= f.startoffset:
			#print start, size, f.recsize, f.startoffset
			##raw_input('in rb.get rows outside loop'+str(f.startoffset))
			i = 0
			if start > f.startoffset:
				temp = f.startoffset
				for i in range(len(f.recsize)):
					if f.recsize[i][1] + temp > start:
						break
					temp += f.recsize[i][1]
			##raw_input('in rb.get rows after if'+str(f.startoffset))
			while size > 0:
				flag = 0
				while i < len(f.recsize):
					if f.recsize[i][1] > size:
						flag = 1
						break
					pk.append(f.recsize[i][0])
					#print 'Added ', f.recsize[i][0]
					
					size -= f.recsize[i][1]
					i += 1
				if flag == 1 or i == len(f.recsize):
					break
			
			start = start + f.size
			##raw_input('in rb.get rows after adding'+str(f.startoffset))
		return pk
	
	def printRI(self):
		pass
		#print "\t\t RI START : ", self.startoffset
		#print "\t\t RI SIZE : ", self.size
		#print "\t\t\t RI PKSET : ", self.pkset	
		 
class offsetBlock:
	ID = 0
	def __init__(self, start):
		self.first = rowIndex(0)
		self.startoffset = start
		self.max = OB
		self.size = 0
		self.next = None
		self.last = self.first
		self.id = offsetBlock.ID + 1
		offsetBlock.ID += 1

	def insert(self, row, pks):
		f = self.first
		size = len(row)
		#if size + self.size <= self.max: 
		while f:
			if f.insert(row, pks) != 0:
				self.size += size
				return 1
			f = f.next
		else:
			temp = rowIndex( self.last.startoffset + self.last.size )
			self.last.next = temp
			self.last = temp
			if temp.insert(row, pks) != 0:
				self.size += size
				return 1
		return 0		

	def delete(self, row, pks):
		f = self.first
		prev = f
		size = len(row)
		while f:
			if f.delete(row, pks) != 0:
				self.size -= size
				if f.size == 0:
					prev.next = f.next
					del f
				return 1
			prev = f
			f = f.next
		return 0
	
	def getStart(self, startoffset, size):
		f = self.first
		while f:
			if f.present(startoffset) :
				return f
			f = f.next
	def getSizelist(self):
		filesize = []
		f = self.first
		while f:
			filesize.append(f.size)
			f = f.next
		return filesize
	def getOffset(self,rbn):
		counter = 1
		f= self.first
		#print rbn
		while f and counter < rbn:
			counter += 1
			f = f.next
		return f.startoffset
			
	def getRowsPart(self, start, startoffset, size):
		pkset = []
		f = start
		##raw_input('in ob.get rows part no loop'+str(f.startoffset))
		pkset.extend(f.getRows(startoffset,size))
		return pkset
	def getRows(self, start, startoffset, size):
		pkset = []
		f = start
		while f and size - f.size >= 0: 
			
			pkset.extend(f.getRows(startoffset, size))
			#print "Added ", pkset
			size -= f.size
			f = f.next
			#print "SIZE", size
		return pkset
		
	def present(self, offset):
		if offset >= self.startoffset and offset < self.startoffset + self.size:
			#print offset,"PRESENT in ",self.id 
			return True
		return False	
	
	def printOB(self):
		#print "\t OB START : ",self.startoffset
		#print "\t OB SIZE : ", self.size
		f = self.first
		while f:
			f.printRI()
			f = f.next
		
class offsetTable:
	def __init__(self, name, start):
		self.name = name
		self.size = 0
		self.first = offsetBlock(start)
		self.last = self.first
		self.counter = 0
		
	def getRowCount(self):
		return self.counter
	
	def insert(self, row, pks):
		f = self.first
		size = len(row)
		while f:
			if f.insert(row, pks) != 0:
				self.size += size
				return 1
			f = f.next
		else:
			temp = offsetBlock(self.last.startoffset + self.last.size)
			self.last.next = temp
			self.last = temp
			if temp.insert(row, pks) != 0:
				self.size += size
				return 1
		return 0
	
	def getOffset(self,rowblocknum):
		f = self.first
		return f.getOffset(rowblocknum + 1)
	def getSizeList(self):
		return self.first.getSizelist()
	def delete(self, row, pks):
		f = self.first
		prev = self.first
		size = len(row)
		while f:
			if f.delete(row, pks) != 0 :
				self.size -= size
				if f.size == 0:
					if not f.next:
						if prev != f:
							prev.next = None
							del f
						else:
							del f
					else:
						prev.next = f.next
						del f
				return 1
			prev = f
			f = f.next
		return 0
	def getRowsPart(self, startoffset, size):
		f = self.first
		pkset = []
		while f:
			#print f.startoffset
			#print "TES"
			##raw_input('in ot.get rows part'+str(f.startoffset))
			if  f.present(startoffset):
				break
			f = f.next
		#print "TEST"
		##raw_input('in ot.get rows part outside loop'+str(f.startoffset))
		r = f.getStart(startoffset, size)
		pks = f.getRowsPart(r, startoffset, size)
		if pks:
			pkset.extend(pks)
			
		return pkset
	def getRows(self, startoffset, size):
		f = self.first
		pkset = []
		while f:
			#print f.startoffset
			#print "TES"
			
			if  f.present(startoffset):
				break
			f = f.next
		#print "TEST"
		
		r = f.getStart(startoffset, f.size)
		while f and size > 0:
			if size > f.size:
				#print "TESTS"
				pks = f.getRows(r, startoffset, f.size)
				if pks:
					pkset.extend(pks)
					size -= f.size
				else:
					break
			else:
				#print "TESTS"
				r = f.getStart(startoffset, size)
				pks = f.getRows(r, startoffset, f.size)
				pkset.extend(pks)
				size = 0
			f = f.next
			if f:
				r = f.first
		return pkset
	def printOT(self):
		#print "OT NAME : ", self.name
		#print "OT SIZE : ", self.size
		f = self.first
		while f:
			f.printOB()
			f = f.next
		

def buildIndex(table, buf, keys):
	counter = 0
	print "Building for ", table
	OT = offsetTable(table, 0)
	
	print table
	#keys = f.getKeys(table)
	ks = []
	for k in range(len(keys)):
		if keys[k] != 'NU&&':
			ks.append(k)	
	for row in buf:
		if row:
			pks = ks
			OT.insert(row,pks)
			counter += 1
			if counter % 1000 == 0:
				print "ADDED ROW ", len(row), OT.size, counter
	OT.printOT()
	'''for row in buf:
		if row:
			cols = row.split(delim)
			##print ks
			pks = ks
		
			OT.delete(row,pks)
	OT.printOT()'''
	##print OT.getRows(2000, 200000)
	#print "Indexed", counter, " records in table ", table
	OT.counter = counter
	return OT,counter
	
def testIndex(db):
	
	tabs = SQL.DB.listtabs('world', db)
	for f in db:
		if str(f) == 'world':
			break
	f.gettableheadings(tabs[0])
	OT = offsetTable(tabs[0], 0)
	keys = f.getKeys(tabs[0])
	ks = []
	for k in range(len(keys)):
		if keys[k] != 'NU&&':
			ks.append(k)
	buf = f.gettableIndex(tabs[0])
	for row in buf:
		if row:
			cols = row.split(delim)
			##print ks
			pks = ks
			OT.insert(row+'\n',pks)
			#print "ADDED ROW ", len(row+'\n'), OT.size
	OT.printOT()
	#for i in range(41):
	#	#print OT.getOffset(i)
	'''for row in buf:
		if row:
			cols = row.split(delim)
			##print ks
			pks = ks
		
			OT.delete(row,pks)
	OT.#printOT()'''
	##print OT.getRows(2000, 200000)
def Logger(fo,content):
	'''Write necessary lof information to file'''
	f = open('/home/mukundan/Documents/FUSESQL/logfile.log','a')
	f.write(fo+'<='+content+"\n")
	f.close()
def SQLLogger(fo,content):
	'''Write the sql commands to the file'''
	f = open('/home/mukundan/Documents/FUSESQL/sql.log','a')
	f.write(fo+'<='+content+"\n")
	f.close()
def PKSetLogger(fo,content):
	'''Write the sql commands to the file'''
	f = open('/home/mukundan/Documents/FUSESQL/pkset.log','a')
	f.write(fo+'<='+content+"\n")
	f.close()

def TruncateFiles():
	open('/home/mukundan/Documents/FUSESQL/logfile.log','w').close()
	open('/home/mukundan/Documents/FUSESQL/sql.log','w').close()
	open('/home/mukundan/Documents/FUSESQL/pkset.log','w').close()

if __name__ == "__main__":	
	print "UTILS"
	##raw_input()
