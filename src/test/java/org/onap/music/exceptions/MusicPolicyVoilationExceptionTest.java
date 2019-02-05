/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 * Copyright (c) 2019 IBM Intellectual Property
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

package org.onap.music.exceptions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MusicPolicyVoilationExceptionTest {

    @Test
    public void TestException1() {
        String s1 = "Value1";
        String s2 = "value2";
        try {
            if (!s1.equalsIgnoreCase(s2)) {
                throw new MusicPolicyVoilationException();
            }
        } catch (MusicPolicyVoilationException mve) {
            assertEquals("org.onap.music.exceptions.MusicPolicyVoilationException", mve.getClass().getName());
        }

    }

    @Test
    public void TestException2() {
        String s1 = "Value1";
        String s2 = "value2";
        try {
            if (!s1.equalsIgnoreCase(s2)) {
                throw new MusicPolicyVoilationException("MusicPolicyVoilationException Exception occured..");
            }
        } catch (MusicPolicyVoilationException mve) {
            assertEquals(mve.getMessage(), "MusicPolicyVoilationException Exception occured..");
        }

    }

    @Test
    public void TestException3() {
        String s1 = "Value1";
        String s2 = "value2";
        try {
            if (!s1.equalsIgnoreCase(s2)) {
                throw new MusicPolicyVoilationException(new Throwable());
            }
        } catch (MusicPolicyVoilationException mve) {
            assertEquals("org.onap.music.exceptions.MusicPolicyVoilationException", mve.getClass().getName());
        }

    }

    @Test
    public void TestException4() {
        String message = "Exception occured";
        String s1 = "Value1";
        String s2 = "value2";
        try {
            if (!s1.equalsIgnoreCase(s2)) {
                throw new MusicPolicyVoilationException(message, new Throwable());
            }
        } catch (MusicPolicyVoilationException mve) {
            assertEquals("org.onap.music.exceptions.MusicPolicyVoilationException", mve.getClass().getName());
        }

    }

    @Test
    public void TestException5() {
        String message = "Exception occured";
        boolean enableSuppression = true;
        boolean writableStackTrace = false;
        String s1 = "Value1";
        String s2 = "value2";
        try {
            if (!s1.equalsIgnoreCase(s2)) {
                throw new MusicPolicyVoilationException(message, new Throwable(), enableSuppression,
                        writableStackTrace);
            }
        } catch (MusicPolicyVoilationException mve) {
            assertEquals("org.onap.music.exceptions.MusicPolicyVoilationException", mve.getClass().getName());
        }

    }

}
