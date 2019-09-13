/*
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
 */
package org.onap.music.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeadlockDetectionUtil {
	private Map<String, Node> nodeList = null;
	public enum OwnershipType {NONE, CREATED, ACQUIRED};

	private class Node implements Comparable<Node> {
		private String id;
		private List<Node> links;
		private boolean visited = false;
		private boolean onStack = false;
		
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			for (Node link : links) sb.append(link.id);
			return "Node [id=" + id + ", links=" + sb.toString() + ", visited=" + visited + ", onStack=" + onStack + "]";
		}

		public Node(String id) {
			super();
			this.id = id;
			this.links = new ArrayList<Node>();
		}

		public List<Node> getLinks() {
			return links;
		}

		public void addLink(Node link) {
			this.links.add(link);
		}
		
		public void removeLink(Node link) {
			this.links.remove(link);
		}

		public boolean isVisited() {
			return visited;
		}

		public boolean isOnStack() {
			return onStack;
		}

		public void setVisited(boolean visited) {
			this.visited = visited;
		}

		public void setOnStack(boolean onStack) {
			this.onStack = onStack;
		}

		@Override
		public int compareTo(Node arg0) {
			return id.compareTo(arg0.id);
		}
	}
	
	public DeadlockDetectionUtil() {
		this.nodeList = new HashMap<String, Node>();
	}

	public void listAllNodes() {
		System.out.println("In DeadlockDetectionUtil: ");
		for (String key : nodeList.keySet()) {
			System.out.println("    " + key + " : " + nodeList.get(key));
		}
	}

	public boolean checkForDeadlock(String resource, String owner, OwnershipType operation) {
		setExisting(resource, owner, operation);

		Node currentNode = null;
		if (operation.equals(OwnershipType.ACQUIRED)) {
			currentNode = nodeList.get("r" + resource);
		} else if (operation.equals(OwnershipType.CREATED)) {
			currentNode = nodeList.get("o" + owner);
		}

		boolean cycle = findCycle(currentNode);
		return cycle;
	}

	private boolean findCycle(Node currentNode) {
		if (currentNode==null) return false;
		if (currentNode.isOnStack()) return true;
		if (currentNode.isVisited()) return false;
		currentNode.setOnStack(true);
		currentNode.setVisited(true);
		for (Node childNode : currentNode.getLinks()) {
			if (findCycle(childNode)) return true;
		}
		currentNode.setOnStack(false);
		return false;
	}

	public void setExisting(String resource, String owner, OwnershipType operation) {
		String resourceKey = "r" + resource;
		Node resourceNode = nodeList.get(resourceKey);
		if (resourceNode==null) {
			resourceNode = new Node(resourceKey);
			nodeList.put(resourceKey, resourceNode);
		}
		
		String ownerKey = "o" + owner;
		Node ownerNode = nodeList.get(ownerKey);
		if (ownerNode==null) {
			ownerNode = new Node(ownerKey);
			nodeList.put(ownerKey, ownerNode);
		}
		
		if (operation.equals(OwnershipType.ACQUIRED)) {
			resourceNode.addLink(ownerNode);
			ownerNode.removeLink(resourceNode);
		} else if (operation.equals(OwnershipType.CREATED)) {
			ownerNode.addLink(resourceNode);
			resourceNode.removeLink(ownerNode);
		}
	}

}
