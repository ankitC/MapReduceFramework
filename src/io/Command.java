package io;

/* List of messages/commands that will be passed around in the framework */
public enum Command {
    HEARTBEAT,
    CURRENT_LOAD,
    DOWNLOAD,
    UPLOAD,
    MAP,
    COMBINE,
    REDUCE,
    CLEANUP,
    SHUTDOWN
}
