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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;

import java.io.Serializable;

/**
 * Hooks provided by users on job status changing. Triggered at the initial(CREATED) and final
 * state(FINISHED/CANCELED/FAILED) of the job.
 *
 * <p>Usage examples: <code>
 *     StreamGraph streamGraph = env.getStreamGraph();
 *     streamGraph.registerJobStatusHook(myJobStatusHook);
 *     streamGraph.setJobName("my_flink");
 *     env.execute(streamGraph);
 * </code>
 */
@Internal
public interface JobStatusHook extends Serializable {

    /** When Job become {@link JobStatus#CREATED} status, it would only be called one time. */
    void onCreated(JobID jobId);

    /** When job finished successfully. */
    void onFinished(JobID jobId);

    /** When job failed finally. */
    void onFailed(JobID jobId, Throwable throwable);

    /** When job get canceled by users. */
    void onCanceled(JobID jobId);
}
