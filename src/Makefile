JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	./Command.java \
	./Config.java \
	./MapTask.java \
	./Master.java \
	./ReduceTask.java \
	./TaskMessage.java \
	./Worker.java \
	./WorkerConfig.java \
	./FileManager.java \
	./IPAddress.java \
	./Pair.java \


RMCLASSES = \
	./Command.java \
	./Config.java \
	./MapTask.java \
	./Master.java \
	./ReduceTask.java \
	./TaskMessage.java \
	./Worker.java \
	./WorkerConfig.java \
	./FileManager.java \
	./IPAddress.java \
	./Pair.java \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
