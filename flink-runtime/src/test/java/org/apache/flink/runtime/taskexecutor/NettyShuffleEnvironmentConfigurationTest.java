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
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
	 * Test for {@link NettyShuffleEnvironmentConfiguration#calculateNetworkBufferMemory(long, Configuration)} using old
	 * configurations via {@link NettyShuffleEnvironmentOptions#NETWORK_NUM_BUFFERS}.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void calculateNetworkBufOld() {
		Configuration config = new Configuration();
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 1);

		// note: actual network buffer memory size is independent of the totalJavaMemorySize
		assertEquals(MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes(),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(10L << 20, config));
		assertEquals(MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes(),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(64L << 20, config));

		// test integer overflow in the memory size
		int numBuffers = (int) ((2L << 32) / MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes()); // 2^33
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, numBuffers);
		assertEquals(2L << 32, NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(2L << 33, config));
	}

	/**
	 * Test for {@link NettyShuffleEnvironmentConfiguration#calculateNetworkBufferMemory(long, Configuration)} using new
	 * configurations via {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_FRACTION},
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MIN} and
	 * {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MAX}.
	 */
	@Test
	public void calculateNetworkBufNew() throws Exception {
		Configuration config = new Configuration();

		// (1) defaults
		final Float defaultFrac = NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.defaultValue();
		final Long defaultMin = MemorySize.parse(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.defaultValue()).getBytes();
		final Long defaultMax = MemorySize.parse(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.defaultValue()).getBytes();
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 20)), defaultMin, defaultMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((64L << 20 + 1), config));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 30)), defaultMin, defaultMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((10L << 30), config));

		calculateNetworkBufNew(config);
	}

	/**
	 * Helper to test {@link NettyShuffleEnvironmentConfiguration#calculateNetworkBufferMemory(long, Configuration)} with the
	 * new configuration parameters.
	 *
	 * @param config configuration object
	 */
	private static void calculateNetworkBufNew(final Configuration config) {
		// (2) fixed size memory
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN, String.valueOf(1L << 20)); // 1MB
		config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(1L << 20)); // 1MB


		// note: actual network buffer memory size is independent of the totalJavaMemorySize
		assertEquals(1 << 20, NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(10L << 20, config));
		assertEquals(1 << 20, NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(64L << 20, config));
		assertEquals(1 << 20, NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(1L << 30, config));

		// (3) random fraction, min, and max values
		Random ran = new Random();
		for (int i = 0; i < 1_000; ++i){
			float frac = Math.max(ran.nextFloat(), Float.MIN_VALUE);
			config.setFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION, frac);

			long min = Math.max(MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes(), ran.nextLong());
			config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN, String.valueOf(min));


			long max = Math.max(min, ran.nextLong());
			config.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(max));

			long javaMem = Math.max(max + 1, ran.nextLong());

			final long networkBufMem = NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(javaMem, config);

			if (networkBufMem < min) {
				fail("Lower bound not met with configuration: " + config.toString());
			}

			if (networkBufMem > max) {
				fail("Upper bound not met with configuration: " + config.toString());
			}

			if (networkBufMem > min && networkBufMem < max) {
				if ((javaMem  * frac) != networkBufMem) {
					fail("Wrong network buffer memory size with configuration: " + config.toString() +
					". Expected value: " + (javaMem * frac) + " actual value: " + networkBufMem + '.');
				}
			}
		}
	}

	/**
	 * Test for {@link NettyShuffleEnvironmentConfiguration#calculateNetworkBufferMemory(long, Configuration)} using mixed
	 * old/new configurations.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void calculateNetworkBufMixed() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 1);

		final Float defaultFrac = NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.defaultValue();
		final Long defaultMin = MemorySize.parse(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.defaultValue()).getBytes();
		final Long defaultMax = MemorySize.parse(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.defaultValue()).getBytes();


		// old + 1 new parameter = new:
		Configuration config1 = config.clone();
		config1.setFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertEquals(enforceBounds((long) (0.1f * (10L << 20)), defaultMin, defaultMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((64L << 20 + 1), config1));
		assertEquals(enforceBounds((long) (0.1f * (10L << 30)), defaultMin, defaultMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((10L << 30), config1));

		config1 = config.clone();
		long newMin = MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes(); // smallest value possible
		config1.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN, String.valueOf(newMin));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 20)), newMin, defaultMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((10L << 20), config1));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 30)), newMin, defaultMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((10L << 30), config1));

		config1 = config.clone();
		long newMax = Math.max(64L << 20 + 1, MemorySize.parse(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.defaultValue()).getBytes());
		config1.setString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(newMax));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 20)), defaultMin, newMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((64L << 20 + 1), config1));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 30)), defaultMin, newMax),
			NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory((10L << 30), config1));
		assertTrue(NettyShuffleEnvironmentConfiguration.hasNewNetworkConfig(config1));

		// old + any new parameter = new:
		calculateNetworkBufNew(config);
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
