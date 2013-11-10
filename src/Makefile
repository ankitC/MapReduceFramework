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
	./mapreduce/MapReduce.java \
	./master/Master.java \
	./master/FileManager.java \
	./master/Scheduler.java \
	./worker/Worker.java \
	./client/Client.java \
	./client/WordCountMapTask.java \
	./client/WordCountReduceTask.java \


RMCLASSES = \
	./common/Pair.java \
	./config/Config.java \
	./config/WorkerConfig.java \
	./io/Command.java \
	./io/IPAddress.java \
	./io/TaskMessage.java \
	./mapreduce/MapTask.java \
	./mapreduce/ReduceTask.java \
	./mapreduce/MapReduce.java \
	./master/Master.java \
	./master/FileManager.java \
	./master/Scheduler.java \
	./worker/Worker.java \
	./client/Client.java \
	./client/WordCountMapTask.java \
	./client/WordCountReduceTask.java \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
