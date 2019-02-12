package org.onap.music.rest.service.impl;

import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.rest.repository.RestMusicQAPIRepository;
import org.onap.music.rest.service.MusicDataAPIService;
import org.onap.music.rest.service.RestMusicQAPIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class RestMusicQAPIServiceImpl implements RestMusicQAPIService{

	@Autowired
	RestMusicQAPIRepository restMusicQAPIRepository;
	
	@Override
	public Response createQueue(String version, String minorVersion, String patchVersion, 
			JsonTable tableObj, String authorization, String aid, String ns, String keyspace, String tablename,MusicDataAPIService musicDataAPIService) {
		return restMusicQAPIRepository.createQueue(version, minorVersion, patchVersion, tableObj, authorization, aid,
				ns, keyspace, tablename, musicDataAPIService);
	}

}
