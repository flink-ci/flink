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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link NettyShuffleEnvironmentConfiguration}.
 */
public class NettyShuffleEnvironmentConfigurationTest extends TestLogger {

	private static final MemorySize MEM_SIZE_PARAM = new MemorySize(128L * 1024 * 1024);

	@Test
	public void testNetworkBufferNumberCalculation() {
		final Configuration config = new Configuration();
		config.setString(TaskManagerOptions.MEMORY_SEGMENT_SIZE, "1m");
		config.setInteger(NettyShuffleEnvironmentOptions.NUM_ARENAS, 1); // 1 x 96Mb = 96Mb
		final int numNetworkBuffers = NettyShuffleEnvironmentConfiguration.fromConfiguration(
			config,
			MEM_SIZE_PARAM,
			false,
			InetAddress.getLoopbackAddress()).numNetworkBuffers();
		assertThat(numNetworkBuffers, is(32)); // 128Mb (total) - 96Mb (arenas) / 1Mb (page) = 32
	}

	/**
	 * Verifies that {@link  NettyShuffleEnvironmentConfiguration#fromConfiguration(Configuration, MemorySize, boolean, InetAddress)}
	 * returns the correct result for new configurations via
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_REQUEST_BACKOFF_INITIAL},
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_REQUEST_BACKOFF_MAX},
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL} and
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}
	 */
	@Test
	public void testNetworkRequestBackoffAndBuffers() {

		// set some non-default values
		final Configuration config = new Configuration();
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, 10);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 100);

		final  NettyShuffleEnvironmentConfiguration networkConfig =  NettyShuffleEnvironmentConfiguration.fromConfiguration(
			config,
			MEM_SIZE_PARAM,
			true,
			InetAddress.getLoopbackAddress());

		assertEquals(networkConfig.partitionRequestInitialBackoff(), 100);
		assertEquals(networkConfig.partitionRequestMaxBackoff(), 200);
		assertEquals(networkConfig.networkBuffersPerChannel(), 10);
		assertEquals(networkConfig.floatingNetworkBuffersPerGate(), 100);
	}

	/**
	 * Returns the value or the lower/upper bound in case the value is less/greater than the lower/upper bound, respectively.
	 *
	 * @param value value to inspect
	 * @param lower lower bound
	 * @param upper upper bound
	 *
	 * @return <tt>min(upper, max(lower, value))</tt>
	 */
	private static long enforceBounds(final long value, final long lower, final long upper) {
		return Math.min(upper, Math.max(lower, value));
	}

	/**
	 * Verifies that {@link NettyShuffleEnvironmentConfiguration#hasNewNetworkConfig(Configuration)}
	 * returns the correct result for old configurations via
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_NUM_BUFFERS}.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void hasNewNetworkBufConfOld() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 1);

		assertFalse(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));
	}

	/**
	 * Verifies that {@link NettyShuffleEnvironmentConfiguration#hasNewNetworkConfig(Configuration)}
	 * returns the correct result for new configurations via
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_FRACTION},
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MIN} and {@link
	 * NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MAX}.
	 */
	@Test
	public void hasNewNetworkBufConfNew() throws Exception {
		Configuration config = new Configuration();
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));

		// fully defined:
		config.setFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, "2048");

		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));

		// partly defined:
		config = new Configuration();
		config.setFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, "1024");
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));

		config = new Configuration();
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));
		config.setFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));

		config = new Configuration();
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, "1024");
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));
	}

	/**
	 * Verifies that {@link NettyShuffleEnvironmentConfiguration#hasNewNetworkConfig(Configuration)}
	 * returns the correct result for mixed old/new configurations.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void hasNewNetworkBufConfMixed() throws Exception {
		Configuration config = new Configuration();
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));

		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 1);
		assertFalse(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config));

		// old + 1 new parameter = new:
		Configuration config1 = config.clone();
		config1.setFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config1));

		config1 = config.clone();
		config1.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config1));

		config1 = config.clone();
		config1.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, "1024");
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config1));
	}
}
