JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	./client/Client.java \
	./client/WordCountMapTask.java \
	./client/WordCountReduceTask.java \
	./common/Pair.java \
	./config/Config.java \
	./config/WorkerConfig.java \
	./io/Command.java \
	./io/IPAddress.java \
	./io/TaskMessage.java \
	./mapreduce/MapReduce.java \
	./mapreduce/MapTask.java \
	./mapreduce/ReduceTask.java \
	./master/FileManager.java \
	./master/Master.java \
	./master/Scheduler.java \
	./worker/ExecuteMap.java \
	./worker/ExecuteReduce.java \
	./worker/Worker.java \


RMCLASSES = \
	./client/Client.java \
	./client/WordCountMapTask.java \
	./client/WordCountReduceTask.java \
	./common/Pair.java \
	./config/Config.java \
	./config/WorkerConfig.java \
	./io/Command.java \
	./io/IPAddress.java \
	./io/TaskMessage.java \
	./mapreduce/MapReduce.java \
	./mapreduce/MapTask.java \
	./mapreduce/ReduceTask.java \
	./master/FileManager.java \
	./master/Master.java \
	./master/Scheduler.java \
	./worker/ExecuteMap.java \
	./worker/ExecuteReduce.java \
	./worker/Worker.java \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
