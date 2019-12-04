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

package org.onap.music.main;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class CipherUtilTest {

	private CipherUtil cipherUtil;

	@Before
	public void setup() {
		cipherUtil = new CipherUtil();
	}

	@Test
	public void testEncryptPKC() {
		String encryptedText = CipherUtil.encryptPKC("This is another string to be encrypted",
				"4BFF9DCCD774F3650E20C4D3F69F8C99");
		System.out.println("*************************" + encryptedText);
		assertEquals(88, encryptedText.length());
	}

	@Test
	public void testDecryptPKC() {
		String encryptedText = CipherUtil.encryptPKC("This is another string to be encrypted",
				"4BFF9DCCD774F3650E20C4D3F69F8C99");
		assertEquals("This is another string to be encrypted",
				CipherUtil.decryptPKC(encryptedText, "4BFF9DCCD774F3650E20C4D3F69F8C99"));
	}

}
