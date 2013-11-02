JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	src/Command.java \ 
	src/Config.java \ 
	src/MapTask.java \ 
	src/Master.java \ 
	src/ReduceTask.java \ 
	src/TaskMessage.java \ 
	src/Worker.java \ 
	src/WorkerConfig.java \ 


RMCLASSES = \
	src/Command.java \ 
	src/Config.java \ 
	src/MapTask.java \ 
	src/Master.java \ 
	src/ReduceTask.java \ 
	src/TaskMessage.java \ 
	src/Worker.java \ 
	src/WorkerConfig.java \ 


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
