package com.shapira.examples.consumer.avroclicks;

/**
 * Created by gwen on 8/5/15.
 */
public class SessionState {
    private long lastConnection;
    private int sessionId;

    public SessionState(long lastConnection, int sessionId) {
        this.lastConnection = lastConnection;
        this.sessionId = sessionId;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public long getLastConnection() {
        return lastConnection;
    }

    public void setLastConnection(long lastConnection) {
        this.lastConnection = lastConnection;
    }
}
