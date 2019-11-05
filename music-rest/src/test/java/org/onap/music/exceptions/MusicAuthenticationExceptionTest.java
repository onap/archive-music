package org.onap.music.exceptions;

import static org.junit.Assert.*;
import org.junit.Test;

public class MusicAuthenticationExceptionTest {
	
	@Test
	public void MusicAuthenticationException() {
		assertNotNull(new MusicAuthenticationException());
	
	}
	
	@Test
	public void MusicAuthenticationExceptionTestString() {
		assertNotNull(new MusicAuthenticationException("JUnit Test"));
	
	}
	
	@Test
	public void MusicAuthenticationExceptionTestThrowable() {
		assertNotNull(new MusicAuthenticationException(new Exception("JUnit Test")));
	
	}
	
	@Test
	public void MusicAuthenticationExceptionTestStringThrowable() {
		assertNotNull(new MusicAuthenticationException("JUnit Test", new Exception("JUnit Test")));
	
	}

}
