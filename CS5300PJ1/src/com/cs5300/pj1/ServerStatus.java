package com.cs5300.pj1;

public class ServerStatus {
	public ServerStatus(String status, long time_stamp) {
       this.status = status;
       this.time_stamp = time_stamp;
    }

    public String getStatus() { return this.status; }
    public long getTimeStamp() { return this.time_stamp; }
    
    public void setStatus(String status) { this.status = status; }
    public void setTimeStamp(long time_stamp) { this.time_stamp = time_stamp; }
    
    private String status;
    private long time_stamp;
}
