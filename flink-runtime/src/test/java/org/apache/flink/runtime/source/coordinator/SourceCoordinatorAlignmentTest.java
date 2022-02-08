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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator.WatermarkAlignmentParams;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Unit tests for watermark alignment of the {@link SourceCoordinator}. */
@SuppressWarnings("serial")
public class SourceCoordinatorAlignmentTest extends SourceCoordinatorTestBase {

    @Test
    public void testWatermarkAlignment() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(1000L, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            reportWatermarkEvent(sourceCoordinator1, 0, 42);
            assertLatestWatermarkAlignmentEvent(0, 1042);

            reportWatermarkEvent(sourceCoordinator1, 1, 44);
            assertLatestWatermarkAlignmentEvent(0, 1042);
            assertLatestWatermarkAlignmentEvent(1, 1042);

            reportWatermarkEvent(sourceCoordinator1, 0, 5000);
            assertLatestWatermarkAlignmentEvent(0, 1044);
            assertLatestWatermarkAlignmentEvent(1, 1044);
        }
    }

    @Test
    public void testWatermarkAlignmentWithTwoGroups() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            long maxDrift = 1000L;
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(maxDrift, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            SourceCoordinator<?, ?> sourceCoordinator2 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(maxDrift, "group2", Long.MAX_VALUE),
                            closeableRegistry);

            reportWatermarkEvent(sourceCoordinator1, 0, 42);
            assertLatestWatermarkAlignmentEvent(0, 1042);

            reportWatermarkEvent(sourceCoordinator2, 1, 44);
            assertLatestWatermarkAlignmentEvent(0, 1042);
            assertLatestWatermarkAlignmentEvent(1, 1044);

            reportWatermarkEvent(sourceCoordinator1, 0, 5000);
            assertLatestWatermarkAlignmentEvent(0, 6000);
            assertLatestWatermarkAlignmentEvent(1, 1044);
        }
    }

    protected SourceCoordinator<?, ?> getAndStartNewSourceCoordinator(
            WatermarkAlignmentParams watermarkAlignmentParams,
            AutoCloseableRegistry closeableRegistry)
            throws Exception {
        SourceCoordinator<?, ?> sourceCoordinator =
                getNewSourceCoordinator(watermarkAlignmentParams);
        closeableRegistry.registerCloseable(sourceCoordinator);
        sourceCoordinator.start();
        setAllReaderTasksReady(sourceCoordinator);

        return sourceCoordinator;
    }

    private void reportWatermarkEvent(
            SourceCoordinator<?, ?> sourceCoordinator1, int subtask, long watermark) {
        sourceCoordinator1.handleEventFromOperator(subtask, new ReportedWatermarkEvent(watermark));
        waitForCoordinatorToProcessActions();
        sourceCoordinator1.announceCombinedWatermark();
    }

    private void assertLatestWatermarkAlignmentEvent(int subtask, long expectedWatermark) {
        List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(subtask);
        assertFalse(events.isEmpty());
        assertEquals(new WatermarkAlignmentEvent(expectedWatermark), events.get(events.size() - 1));
    }
}
