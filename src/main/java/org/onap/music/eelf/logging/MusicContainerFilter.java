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
package org.onap.music.eelf.logging;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import org.springframework.stereotype.Component;


/**
 * This filter filter/modifies outbound http responses just before sending back to client. 
 * 
 * @author sp931a
 *
 */
@Component
public class MusicContainerFilter implements  ContainerResponseFilter {

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicContainerFilter.class);
	
	public MusicContainerFilter() {
		
	}
	
	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {
		logger.info(EELFLoggerDelegate.applicationLogger, "In MusicContainerFilter response filter ::::::::::::::::::::::::");
		
		if (null != EELFLoggerDelegate.mdcGet("transactionId")) {
		    EELFLoggerDelegate.mdcRemove("transactionId");
		} 
		
		if (null != EELFLoggerDelegate.mdcGet("conversationId")) {
		    EELFLoggerDelegate.mdcRemove("conversationId");
		} 
		
		if (null != EELFLoggerDelegate.mdcGet("clientId")) {
		    EELFLoggerDelegate.mdcRemove("clientId");
		} 
			
		if (null != EELFLoggerDelegate.mdcGet("messageId")) {
		    EELFLoggerDelegate.mdcRemove("messageId");
		}
	}
	
}
