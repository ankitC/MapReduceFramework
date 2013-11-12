package io;

/* List of messages/commands that will be passed around in the framework */
public enum Command {
    HEARTBEAT,
    CURRENT_LOAD,
    DOWNLOAD,
    MAP,
    COMBINE,
    REDUCE,
    SHUTDOWN
}
