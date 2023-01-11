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

package org.apache.flink.table.runtime.functions.table.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalCacheMetricGroup;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.LookupFullCache;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.runtime.metrics.groups.InternalCacheMetricGroup.UNINITIALIZED;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper function around user-provided lookup function with a cache layer.
 *
 * <p>This function will check the cache on lookup request and return entries directly on cache hit,
 * otherwise the function will invoke the actual lookup function, and store the entry into the cache
 * after lookup for later use.
 */
@Internal
public class CachingLookupFunction extends LookupFunction {
    private static final long serialVersionUID = 1L;

    // Constants
    public static final String LOOKUP_CACHE_METRIC_GROUP_NAME = "cache";

    // The actual user-provided lookup function
    @Nullable private final LookupFunction delegate;

    private LookupCache cache;
    private transient String cacheIdentifier;

    // Cache metrics
    private transient CacheMetricGroup cacheMetricGroup;
    private transient Counter loadCounter;
    private transient Counter numLoadFailuresCounter;
    private volatile long latestLoadTime = UNINITIALIZED;

    /**
     * Create a {@link CachingLookupFunction}.
     *
     * <p>Please note that the cache may not be the final instance serving in this function. The
     * actual cache instance will be retrieved from the {@link LookupCacheManager} during {@link
     * #open}.
     */
    public CachingLookupFunction(LookupCache cache, @Nullable LookupFunction delegate) {
        this.cache = cache;
        this.delegate = delegate;
    }

    /**
     * Open the {@link CachingLookupFunction}.
     *
     * <p>In order to reduce the memory usage of the cache, {@link LookupCacheManager} is used to
     * provide a shared cache instance across subtasks of this function. Here we use {@link
     * #functionIdentifier()} as the id of the cache, which is generated by MD5 of serialized bytes
     * of this function. As different subtasks of the function will generate the same MD5, this
     * could promise that they will be served with the same cache instance.
     *
     * @see #functionIdentifier()
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        // Get the shared cache from manager
        cacheIdentifier = functionIdentifier();
        cache = LookupCacheManager.getInstance().registerCacheIfAbsent(cacheIdentifier, cache);

        // Register metrics
        cacheMetricGroup =
                new InternalCacheMetricGroup(
                        context.getMetricGroup(), LOOKUP_CACHE_METRIC_GROUP_NAME);
        if (!(cache instanceof LookupFullCache)) {
            loadCounter = new SimpleCounter();
            cacheMetricGroup.loadCounter(loadCounter);
            numLoadFailuresCounter = new SimpleCounter();
            cacheMetricGroup.numLoadFailuresCounter(numLoadFailuresCounter);
        }
        if (cache instanceof LookupFullCache) {
            // TODO add Configuration into FunctionContext
            ((LookupFullCache) cache).setParameters(new Configuration());
        }
        // Initialize cache and the delegating function
        cache.open(cacheMetricGroup);
        if (delegate != null) {
            delegate.open(context);
        }
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        Collection<RowData> cachedValues = cache.getIfPresent(keyRow);
        if (cachedValues != null) {
            // Cache hit
            return cachedValues;
        } else {
            // Cache miss
            Collection<RowData> lookupValues = lookupByDelegate(keyRow);
            // Here we use keyRow as the cache key directly, as keyRow always contains the copy of
            // key fields from left table, no matter if object reuse is enabled.
            if (lookupValues == null || lookupValues.isEmpty()) {
                cache.put(keyRow, Collections.emptyList());
            } else {
                cache.put(keyRow, lookupValues);
            }
            return lookupValues;
        }
    }

    @Override
    public void close() throws Exception {
        if (delegate != null) {
            delegate.close();
        }
        if (cacheIdentifier != null) {
            LookupCacheManager.getInstance().unregisterCache(cacheIdentifier);
        }
    }

    @VisibleForTesting
    public LookupCache getCache() {
        return cache;
    }

    // -------------------------------- Helper functions ------------------------------
    private Collection<RowData> lookupByDelegate(RowData keyRow) throws IOException {
        try {
            Preconditions.checkState(
                    delegate != null,
                    "User's lookup function can't be null, if there are possible cache misses.");
            long loadStart = System.currentTimeMillis();
            Collection<RowData> lookupValues = delegate.lookup(keyRow);
            updateLatestLoadTime(System.currentTimeMillis() - loadStart);
            loadCounter.inc();
            return lookupValues;
        } catch (Exception e) {
            // TODO: Should implement retry on failure logic as proposed in FLIP-234
            numLoadFailuresCounter.inc();
            throw new IOException(String.format("Failed to lookup with key '%s'", keyRow), e);
        }
    }

    private void updateLatestLoadTime(long loadTime) {
        checkNotNull(
                cacheMetricGroup,
                "Could not register metric '%s' as cache metric group is not initialized",
                MetricNames.LATEST_LOAD_TIME);
        // Lazily register the metric
        if (latestLoadTime == UNINITIALIZED) {
            cacheMetricGroup.latestLoadTimeGauge(() -> latestLoadTime);
        }
        latestLoadTime = loadTime;
    }
}
