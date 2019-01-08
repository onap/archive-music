/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2018 IBM.
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

package org.onap.music.spring.data.cassandra;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;

@Configuration
public class CassandraConfig extends AbstractCassandraConfiguration {
	@Autowired
	private Environment env;
	
	private static String KEYSPACE;

	@Override
	public SchemaAction getSchemaAction() {
		return SchemaAction.CREATE_IF_NOT_EXISTS;
	}

	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		String keyspace = env.getProperty("spring.data.cassandra.keyspace-name") ;
		KEYSPACE=keyspace;
		CreateKeyspaceSpecification specification = null;
		specification = CreateKeyspaceSpecification.createKeyspace(KEYSPACE);
		specification.ifNotExists(true);
        
		return Arrays.asList(specification);
	}
	
	 @Bean
	    public CassandraClusterFactoryBean cluster() {
		    String contactPoints = env.getProperty("spring.data.cassandra.contact-points");
		    int port= Integer.parseInt(env.getProperty("spring.data.cassandra.port"));
	        CassandraClusterFactoryBean cluster = new CassandraClusterFactoryBean();
	        cluster.setContactPoints(contactPoints);
	        cluster.setPort(port);
	        cluster.setKeyspaceCreations(getKeyspaceCreations());
	        return cluster;
	    }

	@Override
	public String[] getEntityBasePackages() {
		return new String[] { "org.onap.music" };
	}

	@Override
	public String getKeyspaceName() {
		return KEYSPACE;
	}
}
