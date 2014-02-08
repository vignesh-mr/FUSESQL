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
def TruncateFiles():
	open('/home/mukundan/Documents/FUSESQL/logfile.log','w').close()
	open('/home/mukundan/Documents/FUSESQL/sql.log','w').close()
