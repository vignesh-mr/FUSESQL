def Logger(fo,content):
	f=open('/home/mukundan/Documents/FUSESQL/logfile.log','a')
	f.write(fo+'<='+content+"\n")
	f.close()

