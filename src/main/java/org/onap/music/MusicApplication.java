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

package org.onap.music;


import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.onap.aaf.cadi.PropAccess;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.PropertiesLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
//import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.context.request.RequestContextListener;


@SpringBootApplication(scanBasePackages = { "org.onap.music.rest"})
@EnableAutoConfiguration(exclude={CassandraDataAutoConfiguration.class})
@ComponentScan(value = {"org.onap.music"})
@EnableScheduling
public class MusicApplication extends SpringBootServletInitializer {

    @Autowired
    PropertiesLoader propertyLoader;
    
    
    public static void main(String[] args) {
        System.setProperty("AFT_DME2_CLIENT_IGNORE_SSL_CONFIG","false");
        System.setProperty("AFT_DME2_CLIENT_KEYSTORE","/opt/app/music/etc/truststore2018.jks");
        System.setProperty("AFT_DME2_CLIENT_KEYSTORE_PASSWORD","changeit");
        System.setProperty("AFT_DME2_CLIENT_SSL_INCLUDE_PROTOCOLS","TLSv1.1,TLSv1.2");
        new MusicApplication().configure(new SpringApplicationBuilder(MusicApplication.class)).run(args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        
        return application.sources(MusicApplication.class);
    }
    

    @Bean("loadProperties")
    public void loadProperties() {
        propertyLoader.loadProperties();
    }

    @Autowired
    private ApplicationContext appContext;
    
    @Bean
    @DependsOn("loadProperties")
    public PropAccess propAccess() {
        if(MusicUtil.getIsCadi())
            return new PropAccess(new String[] { "cadi_prop_files=/opt/app/music/etc/cadi.properties" });
        else
            return null;
    }
    
    @Bean(name = "cadiFilter")
    @DependsOn("loadProperties")
    public Filter cadiFilter() throws ServletException {
        propertyLoader.loadProperties();
        if(MusicUtil.getIsCadi()) {
            PropAccess propAccess = propAccess();
            CadiAuthFilter cadiFilter = new CadiAuthFilter(true, propAccess);
            return cadiFilter;
        } else 
            return (ServletRequest request, ServletResponse response, FilterChain chain) -> {
                //do nothing for now.
            };
        
    }

    @Bean
    @DependsOn("loadProperties")
    public FilterRegistrationBean<Filter> cadiFilterRegistration() throws ServletException {
        FilterRegistrationBean<Filter> frb = new FilterRegistrationBean<>();
        frb.setFilter(cadiFilter());
        // The Following Patterns are used to control what APIs will be secure
        // TODO: Make this a configurable item. Build this from an array?
        if(MusicUtil.getIsCadi()) {
            frb.addUrlPatterns(
                "/v2/keyspaces/*",
                "/v2/locks/*",
                "/v3/locks/*",
                "/v2/priorityq/*",
                "/v2/admin/*"
        );
        } else {
            frb.addUrlPatterns("/v0/test");
        }
        frb.setName("cadiFilter");
        frb.setOrder(0);
        return frb;
    }

    @Bean
    @ConditionalOnMissingBean(RequestContextListener.class)
    public RequestContextListener requestContextListener() {
        return new RequestContextListener();
    }
}
