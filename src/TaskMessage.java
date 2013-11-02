import java.util.Map;

public class TaskMessage {
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
