/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM Intellectual Property
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
 */

package org.onap.music.eelf.logging.format;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class AppMessagesTest {
    

    private AppMessages messages;
    
    @Before
    public void setUp() {
       messages= AppMessages.ALREADYEXIST;
    }
    
    @Test
    public void testDetails()
    {
        messages.setDetails("details");
        assertEquals("details", messages.getDetails());
    }
    
    @Test
    public void testResolution()
    {
        messages.setResolution("Resolution");
        assertEquals("Resolution", messages.getResolution());
    }
    
    @Test
    public void testErrorCode()
    {
        messages.setErrorCode("ErrorCode");
        assertEquals("ErrorCode", messages.getErrorCode());
    }
    
    @Test
    public void testErrorDescription()
    {
        messages.setErrorDescription("ErrorDescription");
        assertEquals("ErrorDescription", messages.getErrorDescription());
    }
}
