package io;

import java.io.Serializable;
import java.util.Map;


/* Message format for messages to be exchanged in the framework */
public class TaskMessage implements Serializable {
    private Command command;
    private Map<String, String> args;

    public TaskMessage(Command command, Map<String, String> args) {
        this.command = command;
        this.args = args;
    }

    public Command getCommand() {
        return command;
    }

    public Map<String, String> getArgs() {
        return args;
    }
}
