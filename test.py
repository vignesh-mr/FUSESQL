import DebugInit,sys,os
print os.path.abspath(sys.argv[1])
server=DebugInit.DInit(sys.argv[1])
server.initialize()
a='fdlkjgdf'
b='jdnfdekg'
c='9'
k=38432
def ggg(a,b=4):
	bbb='p'

server.writeCurrentState()

a=raw_input("Enter a:")

server.writeCurrentState()
