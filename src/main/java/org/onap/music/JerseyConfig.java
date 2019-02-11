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

import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import javax.annotation.PostConstruct;
import org.glassfish.jersey.server.ResourceConfig;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicHealthCheckAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import org.onap.music.rest.RestMusicQAPI;
import org.onap.music.rest.RestMusicTestAPI;
import org.onap.music.rest.RestMusicVersionAPI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class JerseyConfig extends ResourceConfig {

  @Value("${spring.jersey.application-path:/}")
  private String apiPath;

  public JerseyConfig() {
    this.registerEndpoints();
  }

  @PostConstruct
  public void init() {
    this.configureSwagger();
  }

  private void registerEndpoints() {
    register(RestMusicAdminAPI.class);
    register(RestMusicDataAPI.class);
    register(RestMusicLocksAPI.class);
    register(RestMusicQAPI.class);
    register(RestMusicTestAPI.class);
    register(RestMusicVersionAPI.class);
    register(RestMusicHealthCheckAPI.class);
  }

  private void configureSwagger() {
    // Available at localhost:port/swagger.json
    this.register(ApiListingResource.class);
    this.register(SwaggerSerializers.class);

    BeanConfig config = new BeanConfig();
    config.setConfigId("MUSIC");
    config.setTitle("MUSIC");
    config.setVersion("v2");
    config.setContact("Thomas Nelson");
    config.setSchemes(new String[]{"http", "https"});
    config.setBasePath("/MUSIC/rest");
    config.setResourcePackage("org.onap.music");
    config.setPrettyPrint(true);
    config.setScan(true);
  }

}
