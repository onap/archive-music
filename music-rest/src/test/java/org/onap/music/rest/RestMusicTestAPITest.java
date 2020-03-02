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
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.main.MusicUtil;

public class RestMusicTestAPITest {

    RestMusicTestAPI restMusicTestAPI;
    
    @Before
    public void setup() {
        restMusicTestAPI = new RestMusicTestAPI();
    }
    
    @Test
    public void testSimpleTests() {
        HttpServletResponse httpServletResponse = Mockito.mock(HttpServletResponse.class);
        doNothing().when(httpServletResponse).addHeader(Mockito.anyString(), Mockito.anyString());
        MusicUtil.setVersion("x.x.x");
        MusicUtil.setBuild("y.y");
        Map<String, HashMap<String, String>> map = restMusicTestAPI.simpleTests(httpServletResponse);
        
        Map<String, String> map1 = map.get("0");
        assertEquals("2", map1.get("1").toString());
        assertEquals("x.x.x", map1.get("Music Version").toString());
        assertEquals("y.y", map1.get("Music Build").toString());
        
        Map<String, String> map2 = map.get("1");
        assertEquals("3", map2.get("2").toString());
        assertEquals("x.x.x", map2.get("Music Version").toString());
        assertEquals("y.y", map2.get("Music Build").toString());
        
        Map<String, String> map3 = map.get("2");
        assertEquals("4", map3.get("3").toString());
        assertEquals("x.x.x", map3.get("Music Version").toString());
        assertEquals("y.y", map3.get("Music Build").toString());
    }
}
