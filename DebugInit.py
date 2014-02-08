import inspect
import DebugFS
import globalLists
import os

def functionInfo(func,level):
	funcInfo=''
	f=inspect.getargspec(func)
	funcInfo += '\t'*level+"Argument List : "+str(f[0])+"\n"
	funcInfo += '\t'*level+"Variable Argument List : "+str(f[1])+"\n"
	funcInfo += '\t'*level+"Keyword Argument List : "+str(f[2])+"\n"
	funcInfo += '\t'*level+"Default Values List : "+str(f[3])+"\n"
	return funcInfo


def formatFrameMetaData(Glo):
	frameInfo=''
	frameInfo+='Package Name : '+str(Glo['__package__'])+"\n"
	frameInfo+='File Name : '+str(Glo['__file__'])+"\n"
	frameInfo+='Module DocString : '+str(Glo['__doc__'])+"\n"
	frameInfo+='Module NameSpace : '+str(Glo['__name__'])+'\n'
	return frameInfo

def inspectPreviousFrames():
	frame=inspect.currentframe()
	framesinfo=inspect.getouterframes(frame)
	framesInfo=[]
	for frames in framesinfo:
		frameInfo=''
		frame=frames[0]
		Glo=frame.f_globals
		if Glo['__name__']!=__name__:

			frameInfo+=formatFrameMetaData(Glo)

			globalLists.initialize()

			for f in Glo:
				if f not in ['__builtins__','__package__','__file__','__doc__','__name__']:
					if inspect.isfunction(Glo[f]):
						funcInfo=''
						funcInfo+=str(f)+" : "+str(Glo[f])+"\n"+functionInfo(Glo[f],level=1)+"\n"
						globalLists.functions.append(funcInfo)
					elif inspect.isclass(Glo[f]):
						classInfo=''
						classInfo+=str(f)+" : "+str(Glo[f])+"\n"
						globalLists.classes.append(classInfo)
					elif inspect.ismodule(Glo[f]):
						moduleInfo=''
						moduleInfo+=str(f)+" : "+str(Glo[f])+"\n"
						globalLists.modules.append(moduleInfo)
					elif inspect.isbuiltin(Glo[f]):
						builtinInfo=''
						builtinInfo+=str(f)+" : "+str(Glo[f])+"\n"
						globalLists.builtins.append(builtinInfo)
					else:
						globalLists.variables.append(str(f)+" : "+str(Glo[f])+"\n")

			globalLists.sortLists()

			frameInfo+=globalLists.formatListData()
			framesInfo.append(frameInfo)

	finalFrameInfo=''
	for framesInfor in framesInfo:
		finalFrameInfo+=framesInfor
	return finalFrameInfo


class DInit:
	def __init__(self,argv):
		self.mountpoint=os.path.abspath(argv)
		self.period=10

	def initialize(self):
		os.system('python DebugFS.py "'+self.mountpoint+'"')

	def writeCurrentState(self):
		self.filename='currentState.frInfo'
		if self.mountpoint[-1]!='/':
			self.absfilename=self.mountpoint+'/'+self.filename
		else:
			self.absfilename=self.mountpoint+self.filename
		self.currentStateFrameInfo=inspectPreviousFrames()
		fhandle=open(self.absfilename,"w")
		fhandle.write(self.currentStateFrameInfo)
		fhandle.close()


	def closeVFS():
		os.system("fusermount -u "+self.mountpoint)
