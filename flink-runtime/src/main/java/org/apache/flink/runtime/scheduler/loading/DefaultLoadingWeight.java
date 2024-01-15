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

package org.apache.flink.runtime.scheduler.loading;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Objects;

/** The default implementation of {@link LoadingWeight} based on task count loading. */
@Internal
public class DefaultLoadingWeight implements LoadingWeight {

    private float tasks;

    public DefaultLoadingWeight(float tasks) {
        Preconditions.checkArgument(tasks >= 0.0f);
        this.tasks = tasks;
    }

    public void incLoading() {
        this.tasks += 1.0f;
    }

    @Override
    public float getLoading() {
        return tasks;
    }

    @Override
    public LoadingWeight merge(LoadingWeight other) {
        if (other == null) {
            return new DefaultLoadingWeight(this.tasks);
        }
        return new DefaultLoadingWeight(tasks + other.getLoading());
    }

    @Override
    public int compareTo(@Nonnull LoadingWeight o) {
        return Float.compare(tasks, o.getLoading());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultLoadingWeight that = (DefaultLoadingWeight) o;
        return Float.compare(tasks, that.tasks) == 0f;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tasks);
    }
}
