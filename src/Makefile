JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	./Command.java \
	./Config.java \
	./FileManager.java \
	./IPAddress.java \
	./MapTask.java \
	./Master.java \
	./Pair.java \
	./ReduceTask.java \
	./TaskMessage.java \
	./Worker.java \
	./WorkerConfig.java \


RMCLASSES = \
	./Command.java \
	./Config.java \
	./FileManager.java \
	./IPAddress.java \
	./MapTask.java \
	./Master.java \
	./Pair.java \
	./ReduceTask.java \
	./TaskMessage.java \
	./Worker.java \
	./WorkerConfig.java \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
