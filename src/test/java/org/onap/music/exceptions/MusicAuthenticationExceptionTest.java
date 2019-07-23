/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
 *  
 *  Modifications Copyright (C) 2019 IBM.
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

import static org.junit.Assert.*;

import org.junit.Test;

public class MusicAuthenticationExceptionTest {

	private static final Throwable Throwable = null;
	MusicAuthenticationException exception;
	
	@Test
	public void TestException1() {
		MusicAuthenticationException exception=new MusicAuthenticationException();
	}

	@Test
	public void TestException2() {
		String message="checking exception";
		MusicAuthenticationException exception=new MusicAuthenticationException(message);
	}
	
	@Test
	public void TestException3() {
		String message="checking exception";
		MusicAuthenticationException exception=new MusicAuthenticationException(Throwable);
	}
	
	@Test
	public void TestException4() {
		String message="checking exception";
		MusicAuthenticationException exception=new MusicAuthenticationException(message, Throwable);
	}
	
	@Test
	public void TestException5() {
		String message="checking exception";
		boolean enableSuppression = true;
		boolean writableStackTrace = false;
		MusicAuthenticationException exception=new MusicAuthenticationException(message,Throwable,enableSuppression,writableStackTrace);
	}
}