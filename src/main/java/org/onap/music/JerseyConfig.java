/* 
 * Copyright 2012-2015 the original author or authors. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */ 
 
package org.onap.music; 
 
import org.glassfish.jersey.server.ResourceConfig;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicHealthCheckAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import org.onap.music.rest.RestMusicQAPI;
import org.onap.music.rest.RestMusicTestAPI;
import org.onap.music.rest.RestMusicVersionAPI;
import org.springframework.stereotype.Component; 
 
@Component 
public class JerseyConfig extends ResourceConfig { 
 
	public JerseyConfig() { 
		register(RestMusicAdminAPI.class); 
		register(RestMusicDataAPI.class); 
		register(RestMusicLocksAPI.class); 
		//register(RestMusicCassaLocksAPI2.class); 
		register(RestMusicQAPI.class); 
		register(RestMusicTestAPI.class); 
		register(RestMusicVersionAPI.class);
		register(RestMusicHealthCheckAPI.class);
		 
	} 
 
}