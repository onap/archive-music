/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
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
package org.onap.music.datastore.jsonobjects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.onap.music.rest.Application;

public class MusicResponse implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<Application> applicationList = new ArrayList<>();
	private String message;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	private String status;

	public void setSucces(boolean isSucces) {
		this.isSucces = isSucces;
	}

	private boolean isSucces;

	public boolean isSucces() {
		return isSucces;
	}

	public void setisSucces(boolean isSucces) {
		this.isSucces = isSucces;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<Application> getApplicationList() {
		return applicationList;
	}

	public void setApplicationList(List<Application> applicationList) {
		this.applicationList = applicationList;
	}

	public void setResposne(String status, String message) {
		this.status = status;
		this.message = message;
	}
	
	public void addAppToList(Application app) {
		applicationList.add(app);
	}
}
