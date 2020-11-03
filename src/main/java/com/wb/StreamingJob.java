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

package com.wb;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


public class StreamingJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		testSplit(env);

		env.execute("Flink Streaming Java API Skeleton");
	}

	public static void testSplit(StreamExecutionEnvironment env){
		DataStreamSource ds = env.fromElements(1,2,3,4,5,6,7,8,9,10);
		ds.split(new OutputSelector<Integer>() {
			@Override
			public Iterable<String> select(Integer value) {
				List<String> out = new ArrayList<>();
				if(value %2==0){
					out.add("odd");
				}else{
					out.add("even");
				}
				return out;
			}
		}).select("odd").print();
	}
}
