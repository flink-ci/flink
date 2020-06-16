/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch5;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticSearchInputFormatBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * IT cases for the {@link ElasticsearchSink}.
 */
public class ElasticsearchSinkITCase<T> extends ElasticsearchSinkTestBase<T, TransportClient, InetSocketAddress> {

	@Test
	public void testElasticsearchSink() throws Exception {
		runElasticsearchSinkTest();
	}

	@Test
	public void testElasticsearchSinkWithCbor() throws Exception {
		runElasticsearchSinkCborTest();
	}

	@Test
	public void testElasticsearchSinkWithSmile() throws Exception {
		runElasticsearchSinkSmileTest();
	}

	@Test
	public void testElasticsearchSinkWithYaml() throws Exception {
		runElasticsearchSinkYamlTest();
	}

	@Test
	public void testNullAddresses() throws Exception {
		runNullAddressesTest();
	}

	@Test
	public void testEmptyAddresses() throws Exception {
		runEmptyAddressesTest();
	}

	@Test
	public void testInvalidElasticsearchCluster() throws Exception {
		runInvalidElasticsearchClusterTest();
	}

	@Test
	public void testElasticsearchInputFormat() throws Exception {
		runElasticsearchSinkTest();
		runElasticSearchInputFormatTest();
	}

	@Override
	protected ElasticSearchInputFormatBase createElasticsearchInputFormat(
		Map<String, String> userConfig,
		DeserializationSchema<T> deserializationSchema,
		String[] fieldNames,
		String index,
		String type,
		long scrollTimeout,
		int scrollMaxSize,
		QueryBuilder predicate,
		int limit) throws Exception {
		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		ElasticSearch5InputFormat builder = new ElasticSearch5InputFormat.Builder()
			.setUserConfig(userConfig)
			.setTransportAddresses(transports)
			.setDeserializationSchema(deserializationSchema)
			.setFieldNames(fieldNames)
			.setIndex(index)
			.setType(type)
			.setScrollTimeout(scrollTimeout)
			.setScrollMaxSize(scrollMaxSize)
			.setPredicate(predicate)
			.setLimit(limit)
			.build();
		return builder;
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, TransportClient> createElasticsearchSink(
		int bulkFlushMaxActions,
		String clusterName,
		List<InetSocketAddress> addresses,
		ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {

		return new ElasticsearchSink<>(
			Collections.unmodifiableMap(createUserConfig(bulkFlushMaxActions, clusterName)),
			addresses,
			elasticsearchSinkFunction);
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, TransportClient> createElasticsearchSinkForEmbeddedNode(
		int bulkFlushMaxActions,
		String clusterName,
		ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) throws Exception {

		return createElasticsearchSinkForNode(
			bulkFlushMaxActions, clusterName, elasticsearchSinkFunction, "127.0.0.1");
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, TransportClient> createElasticsearchSinkForNode(
		int bulkFlushMaxActions,
		String clusterName,
		ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction,
		String ipAddress) throws Exception {

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName(ipAddress), 9300));

		return new ElasticsearchSink<>(
			Collections.unmodifiableMap(createUserConfig(bulkFlushMaxActions, clusterName)),
			transports,
			elasticsearchSinkFunction);
	}
}
