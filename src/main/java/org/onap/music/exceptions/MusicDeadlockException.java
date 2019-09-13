package org.onap.music.exceptions;

public class MusicDeadlockException extends MusicLockingException {

	public String owner = null;
	public String keyspace = null;
	public String table = null;
	public String key = null;
	
	public MusicDeadlockException() {
		super();
	}

	public MusicDeadlockException(String message) {
		super(message);
	}
	
	public MusicDeadlockException(Throwable cause) {
		super(cause);
	}

	public MusicDeadlockException(String message, Throwable cause) {
		super(message, cause);
	}

	public MusicDeadlockException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public void setValues(String owner, String keyspace, String table, String key) {
		this.owner = owner;
		this.keyspace = keyspace;
		this.table = table;
		this.key = key;
	}

	public String getOwner() {
		return owner;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getTable() {
		return table;
	}

	public String getKey() {
		return key;
	}

	
}
