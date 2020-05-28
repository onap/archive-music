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
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Before;
import org.junit.Test;
//import org.junit.experimental.runners.Enclosed;
//import org.junit.runner.RunWith;
import org.onap.music.main.DeadlockDetectionUtil.OwnershipType;

//@RunWith(Enclosed.class)
public class DeadlockDetectionUtilTest {
	private DeadlockDetectionUtil ddu;

	@Before
	public void setup() {
		ddu = new DeadlockDetectionUtil();
	}

	@Test
	public void testListAllNodes() {
		ddu = new DeadlockDetectionUtil();
		ddu.setExisting("r1", "o2", OwnershipType.ACQUIRED);
		ddu.setExisting("r3", "o2", OwnershipType.ACQUIRED);

		ByteArrayOutputStream outContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));
		ddu.listAllNodes();
		assertNotNull(ddu);

		/*
		 * String expectedOutput = "In DeadlockDetectionUtil: \n" +
		 * "		o2 : Node [id=o2, links=r3, visited=false, onStack=false]\n" +
		 * "		r3 : Node [id=r3, links=, visited=false, onStack=false]\n" +
		 * "		r1 : Node [id=r1, links=o2, visited=false, onStack=false]\n";
		 * assertEquals(expectedOutput, outContent.toString());
		 * 
		 * ddu = new DeadlockDetectionUtil(); ddu.setExisting("111", "222",
		 * OwnershipType.CREATED); ddu.setExisting("333", "222", OwnershipType.CREATED);
		 * outContent = new ByteArrayOutputStream(); System.setOut(new
		 * PrintStream(outContent)); ddu.listAllNodes(); expectedOutput =
		 * "In DeadlockDetectionUtil: \n" +
		 * "    o222 : Node [id=o222, links=r111r333, visited=false, onStack=false]\n" +
		 * "    r333 : Node [id=r333, links=, visited=false, onStack=false]\n" +
		 * "    r111 : Node [id=r111, links=, visited=false, onStack=false]";
		 * assertEquals(expectedOutput, outContent.toString());
		 */
	}

	@Test
	public void testcheckForDeadlock() {
		ddu = new DeadlockDetectionUtil();
		ddu.setExisting("111", "222", DeadlockDetectionUtil.OwnershipType.ACQUIRED);
		ddu.setExisting("333", "444", DeadlockDetectionUtil.OwnershipType.ACQUIRED);
		assertEquals(false, ddu.checkForDeadlock("111", "444", DeadlockDetectionUtil.OwnershipType.CREATED));

		ddu = new DeadlockDetectionUtil();
		ddu.setExisting("111", "222", DeadlockDetectionUtil.OwnershipType.ACQUIRED);
		ddu.setExisting("333", "444", DeadlockDetectionUtil.OwnershipType.ACQUIRED);
		ddu.setExisting("333", "222", DeadlockDetectionUtil.OwnershipType.CREATED);
		assertEquals(true, ddu.checkForDeadlock("111", "444", DeadlockDetectionUtil.OwnershipType.CREATED));
	}
}
