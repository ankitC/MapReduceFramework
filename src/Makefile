JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	./common/Pair.java \
	./config/Config.java \
	./config/WorkerConfig.java \
	./io/Command.java \
	./io/IPAddress.java \
	./io/TaskMessage.java \
	./mapreduce/MapTask.java \
	./mapreduce/ReduceTask.java \
	./master/FileManager.java \
	./master/Master.java \
	./worker/Worker.java \


RMCLASSES = \
	./common/Pair.java \
	./config/Config.java \
	./config/WorkerConfig.java \
	./io/Command.java \
	./io/IPAddress.java \
	./io/TaskMessage.java \
	./mapreduce/MapTask.java \
	./mapreduce/ReduceTask.java \
	./master/FileManager.java \
	./master/Master.java \
	./worker/Worker.java \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
