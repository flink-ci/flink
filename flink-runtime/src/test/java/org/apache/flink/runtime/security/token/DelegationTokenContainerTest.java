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

package org.apache.flink.runtime.security.token;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DelegationTokenContainer}. */
class DelegationTokenContainerTest {
    private static final String TOKEN_KEY = "TEST_TOKEN_KEY";
    private static final String TOKEN_VALUE = "TEST_TOKEN_VALUE";

    @Test
    void testRoundTrip() throws Exception {
        final DelegationTokenContainer container = new DelegationTokenContainer();
        container.addToken(TOKEN_KEY, TOKEN_VALUE.getBytes());

        final byte[] containerBytes = InstantiationUtil.serializeObject(container);
        final DelegationTokenContainer deserializedContainer =
                InstantiationUtil.deserializeObject(containerBytes, getClass().getClassLoader());

        final Map<String, byte[]> genericTokens = deserializedContainer.getTokens();
        assertThat(genericTokens).hasSize(1);
        assertThat(genericTokens.get(TOKEN_KEY)).isEqualTo(TOKEN_VALUE.getBytes());
    }

    @Test
    void getTokenShouldReturnNullWhenNoTokens() {
        final DelegationTokenContainer container = new DelegationTokenContainer();

        assertThat(container.getTokens().get(TOKEN_KEY)).isNull();
    }

    @Test
    void hasTokensShouldReturnFalseWhenNoTokens() {
        final DelegationTokenContainer container = new DelegationTokenContainer();

        assertThat(container.hasTokens()).isFalse();
    }

    @Test
    void hasTokensShouldReturnTrueWithGenericToken() {
        final DelegationTokenContainer container = new DelegationTokenContainer();
        container.addToken(TOKEN_KEY, TOKEN_VALUE.getBytes());

        assertThat(container.hasTokens()).isTrue();
    }
}
