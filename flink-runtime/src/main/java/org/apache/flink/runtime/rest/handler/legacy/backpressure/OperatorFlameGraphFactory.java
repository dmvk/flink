/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperatorFlameGraphFactory {

	public static OperatorFlameGraph createStatsFromSample(
		ExecutionJobVertex vertex, StackTraceSample sample) {
		final NodeBuilder root = new NodeBuilder("root");
		for (List<StackTraceElement[]> executions : sample.getStackTraces().values()) {
			for (StackTraceElement[] traces : executions) {
				root.increment();
				NodeBuilder parent = root;
				for (int i = traces.length - 1; i >= 0; i--) {
					final String name =
						traces[i].getClassName() + "." + traces[i].getMethodName() + ":" + traces[i].getLineNumber();
					parent = parent.addChild(name);
				}
			}
		}
		return new OperatorFlameGraph(sample.getEndTime(), buildFlameGraph(root));
	}

	private static OperatorFlameGraph.Node buildFlameGraph(NodeBuilder builder) {
		final List<OperatorFlameGraph.Node> children = new ArrayList<>();
		for (NodeBuilder builderChild : builder.children.values()) {
			children.add(buildFlameGraph(builderChild));
		}
		return new OperatorFlameGraph.Node(
			builder.name,
			builder.value,
			Collections.unmodifiableList(children));
	}

	private static class NodeBuilder {

		private final Map<String, NodeBuilder> children = new HashMap<>();

		private final String name;

		private int value = 0;

		NodeBuilder(String name) {
			this.name = name;
		}

		NodeBuilder addChild(String name) {
			final NodeBuilder child = children.computeIfAbsent(name, NodeBuilder::new);
			child.increment();
			return child;
		}

		void increment() {
			value++;
		}
	}
}
