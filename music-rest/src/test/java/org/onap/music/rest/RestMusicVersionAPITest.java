/*******************************************************************************
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
 *******************************************************************************/
package org.onap.music.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.main.MusicUtil;

public class RestMusicVersionAPITest {
    
    RestMusicVersionAPI restMusicVersionAPI;
    
    @Before
    public void setup() {
        restMusicVersionAPI = new RestMusicVersionAPI();
    }
    
    @Test
    public void testVersion() {
        MusicUtil.setVersion("x.x.x");
        HttpServletResponse httpServletResponse = Mockito.mock(HttpServletResponse.class);
        doNothing().when(httpServletResponse).addHeader(Mockito.anyString(), Mockito.anyString());
        Map<String,Object> map = restMusicVersionAPI.version(httpServletResponse);
        assertEquals("MUSIC:x.x.x", map.get("version").toString());
        assertEquals("SUCCESS", map.get("status").toString());
    }
    
    @Test
    public void testBuild() {
        MusicUtil.setBuild("y.y");
        MusicUtil.setVersion("x.x.x");
        HttpServletResponse httpServletResponse = Mockito.mock(HttpServletResponse.class);
        doNothing().when(httpServletResponse).addHeader(Mockito.anyString(), Mockito.anyString());
        Map<String,Object> map = restMusicVersionAPI.build(httpServletResponse);
        assertEquals("MUSIC:x.x.x", map.get("version").toString());
        assertEquals("SUCCESS", map.get("status").toString());
        assertEquals("MUSIC:y.y", map.get("build").toString());
    }
}
