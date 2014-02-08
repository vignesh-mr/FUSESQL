variables=[]
functions=[]
classes=[]
modules=[]
builtins=[]

def returnFormatString(List,name):
	frameInfo=''
	frameInfo+="List of "+name+" : \n"
	if List:
		for c in List:
			frameInfo+=c+"\n"
	else:
		frameInfo+="None\n"
	return frameInfo

def initialize():
	global variables,functions,classes,modules,builtins
	variables=[]
	functions=[]
	classes=[]
	modules=[]
	builtins=[]

def sortLists():
	global variables,functions,classes,modules,builtins
	variables=sorted(variables)
	classes=sorted(classes)
	functions=sorted(functions)
	modules=sorted(modules)
	builtins=sorted(builtins)

def formatListData():
	global variables,functions,classes,modules,builtins
	frameInfo=''
	frameInfo+=returnFormatString(variables,"Variables")
	frameInfo+=returnFormatString(functions,"Functions")
	frameInfo+=returnFormatString(classes,"Classes")
	frameInfo+=returnFormatString(modules,"Modules")
	frameInfo+=returnFormatString(builtins,"Builtins")
	return frameInfo
