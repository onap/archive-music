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

package org.onap.music.unittests.jsonobjects;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.onap.music.datastore.jsonobjects.NameSpace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NameSpaceTest {
    private NameSpace nameSpace;

    @Before
    public void setUp() {
        nameSpace = new NameSpace();
    }

    @Test
    public void testGetSetAdmin() {
        List<String> list = new ArrayList<String>();
        list.add("admin");
        nameSpace.setAdmin(list);
        Assert.assertEquals(list, nameSpace.getAdmin());
    }

    @Test
    public void testGetSetName() {
        nameSpace.setName("name");
        Assert.assertEquals("name", nameSpace.getName());
    }
}
