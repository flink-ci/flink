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

package org.apache.flink.table.planner.runtime.stream.sql.join;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThatList;

/**
 * End-to-end result tests for the {@code LATERAL SNAPSHOT} processing-time temporal table join.
 *
 * <p>Processing-time semantics make the join non-deterministic in general. To get stable results,
 * each build source appends a non-matching "flip-trigger" row at the flip timestamp ({@link
 * #MID_FLIP}) while all real build rows are earlier. With per-record ({@code on-event}) watermarks
 * the operator flips to the JOIN phase.
 *
 * <p>Some tests throttle source emission ({@code source.sleep-*}) to place probes deterministically
 * around the flip. Purely processing-time-driven behavior (idle-timeout flip, state-TTL eviction)
 * is covered deterministically by {@code LateralSnapshotJoinOperatorTest}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class LateralSnapshotJoinITCase extends StreamingWithStateTestBase {

    /** The {@code 'user_time'} condition reached mid-stream by the build-side flip-trigger row. */
    private static final String MID_FLIP =
            "load_completed_condition => 'user_time', "
                    + "load_completed_time => CAST(TIMESTAMP '2020-01-01 00:00:10' AS TIMESTAMP_LTZ(3))";

    /** A far-future flip condition: the flip happens only at end of all input. */
    private static final String END_FLIP =
            "load_completed_condition => 'user_time', "
                    + "load_completed_time => CAST(TIMESTAMP '2100-01-01 00:00:00' AS TIMESTAMP_LTZ(3))";

    /** Event time of the flip-trigger row; equal to the {@link #MID_FLIP} timestamp. */
    private static final String FLIP_TRIGGER_TS = "00:00:10";

    /** A build-side key that never matches any probe row. */
    private static final String FLIP_TRIGGER_KEY = "__flip_trigger__";

    public LateralSnapshotJoinITCase(StateBackendMode state) {
        super(state);
    }

    @BeforeEach
    @Override
    public void before() {
        super.before();
        env().setParallelism(1);
        tEnv().getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Parameters(name = "StateBackend={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {StreamingWithStateTestBase.HEAP_BACKEND()},
                    {StreamingWithStateTestBase.ROCKSDB_BACKEND()}
                });
    }

    // ------------------------------------------------------------------------------------------
    // Core join semantics
    // ------------------------------------------------------------------------------------------

    @TestTemplate
    void testInnerJoin() {
        // Throttle the probe so the fast build flips first; the probes are then joined live in the
        // JOIN phase (the build-side is finalized, so buffered-vs-live does not change the result).
        createProbe(defaultProbe(), 40L);
        createAppendBuild(defaultBuild());

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, probe.pv, s.bk, s.bv "
                                + "FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of("a", 100, "a", 10),
                        Row.of("a", 100, "a", 11),
                        Row.of("b", 200, "b", 20));
    }

    @TestTemplate
    void testLeftJoin() {
        createProbe(defaultProbe(), 40L);
        createAppendBuild(defaultBuild());

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, probe.pv, s.bk, s.bv "
                                + "FROM probe LEFT JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of("a", 100, "a", 10),
                        Row.of("a", 100, "a", 11),
                        Row.of("b", 200, "b", 20),
                        Row.of("c", 300, null, null));
    }

    @TestTemplate
    void testSelectStarMaterializesBuildRowtime() {
        createProbe(List.of(Row.of("a", 100, ts("00:01:00"))));
        createAppendBuild(List.of(Row.of("a", 10, ts("00:00:01"))));

        // SELECT * yields probe columns then build columns; the build rowtime is materialized as a
        // regular TIMESTAMP(3), not a time attribute.
        final List<Row> actual =
                collect(
                        "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of(
                                "a",
                                100,
                                LocalDateTime.parse("2020-01-01T00:01:00"),
                                "a",
                                10,
                                LocalDateTime.parse("2020-01-01T00:00:01")));
    }

    @TestTemplate
    void testCompositeKeys() {
        createProbe(
                Arrays.asList(
                        Row.of("a", 10, ts("00:01:00")),
                        Row.of("a", 11, ts("00:01:01")),
                        Row.of("z", 10, ts("00:01:01")),
                        Row.of("b", 20, ts("00:01:02"))));
        createAppendBuild(
                Arrays.asList(
                        Row.of("a", 10, ts("00:00:01")),
                        Row.of("a", 99, ts("00:00:02")),
                        Row.of("b", 20, ts("00:00:03"))));

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, probe.pv, s.bk, s.bv "
                                + "FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk AND probe.pv = s.bv");

        assertThatList(actual)
                .containsExactlyInAnyOrder(Row.of("a", 10, "a", 10), Row.of("b", 20, "b", 20));
    }

    @TestTemplate
    void testNonEquiCondition() {
        createProbe(Arrays.asList(Row.of("a", 15, ts("00:01:00")), Row.of("a", 5, ts("00:01:01"))));
        createAppendBuild(
                Arrays.asList(Row.of("a", 10, ts("00:00:01")), Row.of("a", 20, ts("00:00:02"))));

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, probe.pv, s.bv "
                                + "FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk AND probe.pv > s.bv");

        assertThatList(actual).containsExactlyInAnyOrder(Row.of("a", 15, 10));
    }

    @TestTemplate
    void testEmptyBuildSide() {
        createProbe(defaultProbe());
        createAppendBuild(Collections.emptyList());

        final List<Row> inner =
                collect(
                        "SELECT probe.pk, s.bk FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");
        assertThatList(inner).isEmpty();

        final List<Row> left =
                collect(
                        "SELECT probe.pk, s.bk FROM probe LEFT JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");
        assertThatList(left)
                .containsExactlyInAnyOrder(Row.of("a", null), Row.of("b", null), Row.of("c", null));
    }

    // ------------------------------------------------------------------------------------------
    // Build-side changelog consolidation (all changes have rowtime < the flip ts)
    // ------------------------------------------------------------------------------------------

    @TestTemplate
    void testRetractingBuildChangelog() {
        createProbe(
                Arrays.asList(Row.of("a", 100, ts("00:01:00")), Row.of("b", 200, ts("00:01:01"))));
        // Key a is updated 10 -> 11; key b is inserted then deleted.
        createChangelogBuild(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "a", 10, ts("00:00:01")),
                        Row.ofKind(RowKind.INSERT, "b", 20, ts("00:00:03")),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "a", 10, ts("00:00:01")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "a", 11, ts("00:00:02")),
                        Row.ofKind(RowKind.DELETE, "b", 20, ts("00:00:03"))));

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual).containsExactlyInAnyOrder(Row.of("a", 11));
    }

    @TestTemplate
    void testUpsertBuildSource() {
        createProbe(
                Arrays.asList(Row.of("a", 100, ts("00:01:00")), Row.of("b", 200, ts("00:01:01"))));
        // Upsert source (I,UA,D with PK): a is upserted 10 -> 11, b is deleted.
        createUpsertBuild(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "a", 10, ts("00:00:01")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "a", 11, ts("00:00:02")),
                        Row.ofKind(RowKind.INSERT, "b", 20, ts("00:00:03")),
                        Row.ofKind(RowKind.DELETE, "b", 20, ts("00:00:04"))));

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual).containsExactlyInAnyOrder(Row.of("a", 11));
    }

    // ------------------------------------------------------------------------------------------
    // Flip timing (when LOAD flips to JOIN, and probe placement around the flip)
    // ------------------------------------------------------------------------------------------

    @TestTemplate
    void testInnerJoinFlipsAtEndOfInput() {
        // Far-future flip condition: the operator stays in LOAD and flips only at end-of-input,
        // buffering and draining every probe. The frozen snapshot and result match the mid-flip
        // case.
        createProbe(defaultProbe());
        createAppendBuild(defaultBuild());

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, probe.pv, s.bk, s.bv "
                                + "FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + END_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of("a", 100, "a", 10),
                        Row.of("a", 100, "a", 11),
                        Row.of("b", 200, "b", 20));
    }

    @TestTemplate
    void testDefaultCompileTimeCondition() {
        // No options: 'load_completed_condition' defaults to 'compile_time', so the flip ts is the
        // wall-clock time at planning. The 2020 build watermarks never reach it, so the flip
        // happens at end-of-input.
        createProbe(defaultProbe());
        createAppendBuild(defaultBuild());

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, probe.pv, s.bk, s.bv "
                                + "FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b"
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of("a", 100, "a", 10),
                        Row.of("a", 100, "a", 11),
                        Row.of("b", 200, "b", 20));
    }

    @TestTemplate
    void testProbesJoinedLiveInJoinPhase() {
        // Fast build flips almost immediately; the throttled probe stream is consumed entirely in
        // the JOIN phase, exercising the live per-record path (including a non-matching key). The
        // snapshot is static after the flip, so the result is deterministic.
        final List<Row> probes =
                Arrays.asList(
                        Row.of("a", 1, ts("00:00:01")),
                        Row.of("b", 2, ts("00:00:02")),
                        Row.of("c", 3, ts("00:00:03")), // non-matching
                        Row.of("a", 4, ts("00:00:04")),
                        Row.of("b", 5, ts("00:00:05")),
                        Row.of("c", 6, ts("00:00:06"))); // non-matching
        createProbe(probes, 40L);
        createAppendBuild(
                Arrays.asList(Row.of("a", 10, ts("00:00:01")), Row.of("b", 20, ts("00:00:02"))));

        final List<Row> actual =
                collect(
                        "SELECT probe.pv, probe.pk, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual)
                .containsExactlyInAnyOrder(
                        Row.of(1, "a", 10),
                        Row.of(4, "a", 10),
                        Row.of(2, "b", 20),
                        Row.of(5, "b", 20));
    }

    @TestTemplate
    void testManyProbesBufferedDuringLoadThenDrained() {
        // Throttled build flips at ~240 ms, by which time the un-throttled probe stream is fully
        // buffered in LOAD. The flip drains all buffered probes against the final loaded version;
        // all must match and none may be lost.
        final int probeCount = 20;
        final List<Row> probes = new ArrayList<>();
        for (int i = 1; i <= probeCount; i++) {
            probes.add(Row.of("k", i, ts(String.format("00:00:%02d", i))));
        }
        createProbe(probes); // un-throttled: buffered within a few ms, long before the flip
        createUpsertBuild(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "k", 1, ts("00:00:01")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 2, ts("00:00:02")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 3, ts("00:00:05")),
                        Row.ofKind(RowKind.INSERT, FLIP_TRIGGER_KEY, 0, ts(FLIP_TRIGGER_TS))),
                80L); // ~240 ms until the flip trigger row is emitted

        final List<Row> actual =
                collect(
                        "SELECT probe.pv, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThat(actual).hasSize(probeCount);
        assertThat(actual).allSatisfy(r -> assertThat(r.getField(1)).isEqualTo(3));
    }

    @TestTemplate
    void testProbesObserveProgressiveBuildVersions() {
        // Throttled build applies v2, v3, v4 progressively after the flip while a throttled probe
        // stream spans the progression. Observed versions are non-decreasing; the first probe sees
        // v1 and the last sees v4. Exact per-probe versions are not asserted (source timing and
        // post-flip visibility latency are not precise enough across environments).
        final int probeCount = 8;
        final List<Row> probes =
                IntStream.range(0, probeCount)
                        .mapToObj(i -> Row.of("k", i, ts(String.format("00:00:%02d", i))))
                        .toList();
        createProbe(probes, 200L); // last probe at ~1400 ms, well past the last post-flip update
        createUpsertBuild(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "k", 1, ts("00:00:01")),
                        Row.ofKind(RowKind.INSERT, FLIP_TRIGGER_KEY, 0, ts(FLIP_TRIGGER_TS)),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 2, ts("00:00:20")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 3, ts("00:00:30")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 4, ts("00:00:40"))),
                50L);

        final List<Row> byProbeId =
                sortedByProbeId(
                        collect(
                                "SELECT probe.pv, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b, "
                                        + MID_FLIP
                                        + ")) AS s ON probe.pk = s.bk"));

        assertThat(byProbeId).hasSize(probeCount);
        assertMonotonicVersions(byProbeId);
        assertThat((Integer) byProbeId.get(0).getField(1)).isEqualTo(1);
        assertThat((Integer) byProbeId.get(probeCount - 1).getField(1)).isEqualTo(4);
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private List<Row> defaultProbe() {
        return Arrays.asList(
                Row.of("a", 100, ts("00:01:00")),
                Row.of("b", 200, ts("00:01:01")),
                Row.of("c", 300, ts("00:01:02")));
    }

    private List<Row> defaultBuild() {
        return Arrays.asList(
                Row.of("a", 10, ts("00:00:01")),
                Row.of("b", 20, ts("00:00:02")),
                Row.of("a", 11, ts("00:00:03")));
    }

    private List<Row> collect(String query) {
        return CollectionUtil.iteratorToList(tEnv().executeSql(query).collect());
    }

    /**
     * Appends a non-matching build row at the {@link #MID_FLIP} timestamp; its watermark flips the
     * operator to the JOIN phase mid-stream (all real build rows are earlier).
     */
    private List<Row> withFlipTrigger(List<Row> data) {
        final List<Row> withTrigger = new ArrayList<>(data);
        withTrigger.add(Row.of(FLIP_TRIGGER_KEY, 0, ts(FLIP_TRIGGER_TS)));
        return withTrigger;
    }

    private void createProbe(List<Row> data) {
        createProbe(data, 0L);
    }

    private void createProbe(List<Row> data, long sleepMillis) {
        final String id = TestValuesTableFactory.registerData(data);
        tEnv().executeSql(
                        "CREATE TABLE probe ("
                                + "  pk STRING,"
                                + "  pv INT,"
                                + "  pts TIMESTAMP(3),"
                                + "  WATERMARK FOR pts AS pts"
                                + ") WITH ("
                                + valuesOptions(id, sleepMillis)
                                + ")");
    }

    private void createAppendBuild(List<Row> data) {
        final String id = TestValuesTableFactory.registerData(withFlipTrigger(data));
        tEnv().executeSql(
                        "CREATE TABLE b ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3),"
                                + "  WATERMARK FOR bts AS bts"
                                + ") WITH ("
                                + valuesOptions(id, 0L)
                                + ")");
    }

    private void createChangelogBuild(List<Row> data) {
        final String id = TestValuesTableFactory.registerData(withFlipTrigger(data));
        tEnv().executeSql(
                        "CREATE TABLE b ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3),"
                                + "  WATERMARK FOR bts AS bts"
                                + ") WITH ("
                                + valuesOptions(id, 0L, "'changelog-mode' = 'I,UB,UA,D'")
                                + ")");
    }

    private void createUpsertBuild(List<Row> data) {
        createUpsertBuild(withFlipTrigger(data), 0L);
    }

    private void createUpsertBuild(List<Row> data, long sleepMillis) {
        final String id = TestValuesTableFactory.registerData(data);
        tEnv().executeSql(
                        "CREATE TABLE b ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3),"
                                + "  WATERMARK FOR bts AS bts,"
                                + "  PRIMARY KEY (bk) NOT ENFORCED"
                                + ") WITH ("
                                + valuesOptions(id, sleepMillis, "'changelog-mode' = 'I,UA,D'")
                                + ")");
    }

    private static String valuesOptions(String id, long sleepMillis, String... extraOptions) {
        final StringBuilder options =
                new StringBuilder()
                        .append("  'connector' = 'values',")
                        .append("  'bounded' = 'false',")
                        .append("  'disable-lookup' = 'true',")
                        .append("  'enable-watermark-push-down' = 'true',")
                        .append("  'scan.watermark.emit.strategy' = 'on-event',");
        for (String extra : extraOptions) {
            options.append(String.format("  %s,", extra));
        }
        if (sleepMillis > 0) {
            options.append("  'source.sleep-after-elements' = '1',")
                    .append(String.format("  'source.sleep-time' = '%sms',", sleepMillis));
        }
        return options.append(String.format("  'data-id' = '%s'", id)).toString();
    }

    /** Sorts join results (pv, bv) by the probe id pv, i.e. into probe processing order. */
    private static List<Row> sortedByProbeId(List<Row> results) {
        return results.stream()
                .sorted(Comparator.comparingInt(r -> r.<Integer>getFieldAs(0)))
                .collect(Collectors.toList());
    }

    /** Asserts the build version (field 1) is non-decreasing across the probe-ordered results. */
    private static void assertMonotonicVersions(List<Row> byProbeId) {
        int previous = Integer.MIN_VALUE;
        for (Row r : byProbeId) {
            final int version = r.<Integer>getFieldAs(1);
            assertThat(version).isGreaterThanOrEqualTo(previous);
            previous = version;
        }
    }

    private static LocalDateTime ts(String time) {
        return LocalDateTime.parse("2020-01-01T" + time);
    }
}
