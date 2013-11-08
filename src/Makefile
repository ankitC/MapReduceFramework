JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	./io.Command.java \
	./config.Config.java \
	./mapreduce.MapTask.java \
	./master.Master.java \
	./mapreduce.ReduceTask.java \
	./io.TaskMessage.java \
	./worker.Worker.java \
	./config.WorkerConfig.java \
	./master.FileManager.java \
	./io.IPAddress.java \
	./common.Pair.java \


RMCLASSES = \
	./io.Command.java \
	./config.Config.java \
	./mapreduce.MapTask.java \
	./master.Master.java \
	./mapreduce.ReduceTask.java \
	./io.TaskMessage.java \
	./worker.Worker.java \
	./config.WorkerConfig.java \
	./master.FileManager.java \
	./io.IPAddress.java \
	./common.Pair.java \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
