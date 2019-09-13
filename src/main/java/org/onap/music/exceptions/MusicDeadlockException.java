/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * ============LICENSE_END=============================================
 * ====================================================================
 */
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
