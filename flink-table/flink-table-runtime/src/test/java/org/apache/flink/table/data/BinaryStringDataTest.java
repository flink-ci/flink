/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.operators.sort.SortUtil;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

import org.apache.commons.lang3.StringEscapeUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.table.data.binary.BinaryStringData.blankString;
import static org.apache.flink.table.data.binary.BinaryStringData.fromBytes;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.EMPTY_STRING_ARRAY;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.concat;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.concatWs;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.keyValue;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.reverse;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.splitByWholeSeparatorPreserveAllTokens;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.substringSQL;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toByte;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toDecimal;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toInt;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toLong;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toShort;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trim;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trimLeft;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trimRight;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test of {@link BinaryStringData}.
 *
 * <p>Caution that you must construct a string by {@link TestSpec#fromString} to cover all the test
 * cases.
 */
class BinaryStringDataTest {

    private static Stream<TestSpec> getVarSeg() {
        return Stream.of(
                new TestSpec(Mode.ONE_SEG),
                new TestSpec(Mode.MULTI_SEGS),
                new TestSpec(Mode.STRING),
                new TestSpec(Mode.RANDOM));
    }

    private enum Mode {
        ONE_SEG,
        MULTI_SEGS,
        STRING,
        RANDOM
    }

    static class TestSpec {
        final BinaryStringData empty;
        final Mode mode;

        TestSpec(Mode mode) {
            this.mode = mode;
            empty = fromString("");
        }

        @Override
        public String toString() {
            return "{" + "mode=" + mode + '}';
        }

        BinaryStringData fromString(String str) {
            BinaryStringData string = BinaryStringData.fromString(str);

            Mode mode = this.mode;

            if (mode == Mode.RANDOM) {
                int rnd = new Random().nextInt(3);
                if (rnd == 0) {
                    mode = Mode.ONE_SEG;
                } else if (rnd == 1) {
                    mode = Mode.MULTI_SEGS;
                } else if (rnd == 2) {
                    mode = Mode.STRING;
                }
            }

            if (mode == Mode.STRING) {
                return string;
            }
            if (mode == Mode.ONE_SEG || string.getSizeInBytes() < 2) {
                string.ensureMaterialized();
                return string;
            } else {
                int numBytes = string.getSizeInBytes();
                int pad = new Random().nextInt(5);
                int numBytesWithPad = numBytes + pad;
                int segSize = numBytesWithPad / 2 + 1;
                byte[] bytes1 = new byte[segSize];
                byte[] bytes2 = new byte[segSize];
                if (segSize - pad > 0 && numBytes >= segSize - pad) {
                    string.getSegments()[0].get(0, bytes1, pad, segSize - pad);
                }
                string.getSegments()[0].get(segSize - pad, bytes2, 0, numBytes - segSize + pad);
                return BinaryStringData.fromAddress(
                        new MemorySegment[] {
                            MemorySegmentFactory.wrap(bytes1), MemorySegmentFactory.wrap(bytes2)
                        },
                        pad,
                        numBytes);
            }
        }

        void checkBasic(String str, int len) {
            BinaryStringData s1 = fromString(str);
            BinaryStringData s2 = fromBytes(str.getBytes(UTF_8));
            assertThat(len).isEqualTo(s1.numChars()).isEqualTo(s2.numChars());

            assertThat(str).isEqualTo(s1.toString()).isEqualTo(s2.toString());
            assertThat(s2).isEqualTo(s1).hasSameHashCodeAs(s1);

            assertThat(s1.compareTo(s2)).isEqualTo(0);

            assertThat(s1.contains(s2)).isTrue();
            assertThat(s2.contains(s1)).isTrue();
            assertThat(s1.startsWith(s1)).isTrue();
            assertThat(s1.endsWith(s1)).isTrue();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void basicTest(TestSpec testSpec) {
        testSpec.checkBasic("", 0);
        testSpec.checkBasic(",", 1);
        testSpec.checkBasic("hello", 5);
        testSpec.checkBasic("hello world", 11);
        testSpec.checkBasic("Flink中文社区", 9);
        testSpec.checkBasic("中 文 社 区", 7);

        testSpec.checkBasic("¡", 1); // 2 bytes char
        testSpec.checkBasic("ку", 2); // 2 * 2 bytes chars
        testSpec.checkBasic("︽﹋％", 3); // 3 * 3 bytes chars
        testSpec.checkBasic("\uD83E\uDD19", 1); // 4 bytes char
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void emptyStringTest(TestSpec testSpec) {
        assertThat(testSpec.fromString("")).isEqualTo(testSpec.empty);
        assertThat(fromBytes(new byte[0])).isEqualTo(testSpec.empty);
        assertThat(testSpec.empty.numChars()).isEqualTo(0);
        assertThat(testSpec.empty.getSizeInBytes()).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void compareTo(TestSpec testSpec) {
        assertThat(testSpec.fromString("   ")).isEqualByComparingTo(blankString(3));
        assertThat(testSpec.fromString("")).isLessThan(testSpec.fromString("a"));
        assertThat(testSpec.fromString("abc")).isGreaterThan(testSpec.fromString("ABC"));
        assertThat(testSpec.fromString("abc0")).isGreaterThan(testSpec.fromString("abc"));
        assertThat(testSpec.fromString("abcabcabc"))
                .isEqualByComparingTo(testSpec.fromString("abcabcabc"));
        assertThat(testSpec.fromString("aBcabcabc"))
                .isGreaterThan(testSpec.fromString("Abcabcabc"));
        assertThat(testSpec.fromString("Abcabcabc")).isLessThan(testSpec.fromString("abcabcabC"));
        assertThat(testSpec.fromString("abcabcabc"))
                .isGreaterThan(testSpec.fromString("abcabcabC"));

        assertThat(testSpec.fromString("abc")).isLessThan(testSpec.fromString("世界"));
        assertThat(testSpec.fromString("你好")).isGreaterThan(testSpec.fromString("世界"));
        assertThat(testSpec.fromString("你好123")).isGreaterThan(testSpec.fromString("你好122"));

        MemorySegment segment1 = MemorySegmentFactory.allocateUnpooledSegment(1024);
        MemorySegment segment2 = MemorySegmentFactory.allocateUnpooledSegment(1024);
        SortUtil.putStringNormalizedKey(testSpec.fromString("abcabcabc"), segment1, 0, 9);
        SortUtil.putStringNormalizedKey(testSpec.fromString("abcabcabC"), segment2, 0, 9);
        assertThat(segment1.compare(segment2, 0, 0, 9)).isGreaterThan(0);
        SortUtil.putStringNormalizedKey(testSpec.fromString("abcab"), segment1, 0, 9);
        assertThat(segment1.compare(segment2, 0, 0, 9)).isLessThan(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testMultiSegments(TestSpec testSpec) {

        // prepare
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[10]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[10]);
        segments1[0].put(5, "abcde".getBytes(UTF_8), 0, 5);
        segments1[1].put(0, "aaaaa".getBytes(UTF_8), 0, 5);

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegmentFactory.wrap(new byte[5]);
        segments2[1] = MemorySegmentFactory.wrap(new byte[5]);
        segments2[0].put(0, "abcde".getBytes(UTF_8), 0, 5);
        segments2[1].put(0, "b".getBytes(UTF_8), 0, 1);

        // test go ahead both
        BinaryStringData binaryString1 = BinaryStringData.fromAddress(segments1, 5, 10);
        BinaryStringData binaryString2 = BinaryStringData.fromAddress(segments2, 0, 6);
        assertThat(binaryString1).hasToString("abcdeaaaaa");
        assertThat(binaryString2).hasToString("abcdeb");
        assertThat(binaryString1).isLessThan(binaryString2);

        // test needCompare == len
        binaryString1 = BinaryStringData.fromAddress(segments1, 5, 5);
        binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
        assertThat(binaryString1).hasToString("abcde");
        assertThat(binaryString2).hasToString("abcde");
        assertThat(binaryString1).isEqualByComparingTo(binaryString2);

        // test find the first segment of this string
        binaryString1 = BinaryStringData.fromAddress(segments1, 10, 5);
        binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
        assertThat(binaryString1).hasToString("aaaaa");
        assertThat(binaryString2).hasToString("abcde");
        assertThat(binaryString1).isLessThan(binaryString2);
        assertThat(binaryString2).isGreaterThan(binaryString1);

        // test go ahead single
        segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(new byte[10])};
        segments2[0].put(4, "abcdeb".getBytes(UTF_8), 0, 6);
        binaryString1 = BinaryStringData.fromAddress(segments1, 5, 10);
        binaryString2 = BinaryStringData.fromAddress(segments2, 4, 6);
        assertThat(binaryString1).hasToString("abcdeaaaaa");
        assertThat(binaryString2).hasToString("abcdeb");
        assertThat(binaryString1).isLessThan(binaryString2);
        assertThat(binaryString2).isGreaterThan(binaryString1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void concatTest(TestSpec testSpec) {
        assertThat(concat()).isEqualTo(testSpec.empty);
        assertThat(concat((BinaryStringData) null)).isNull();
        assertThat(concat(testSpec.empty)).isEqualTo(testSpec.empty);
        assertThat(concat(testSpec.fromString("ab"))).isEqualTo(testSpec.fromString("ab"));
        assertThat(concat(testSpec.fromString("a"), testSpec.fromString("b")))
                .isEqualTo(testSpec.fromString("ab"));
        assertThat(
                        concat(
                                testSpec.fromString("a"),
                                testSpec.fromString("b"),
                                testSpec.fromString("c")))
                .isEqualTo(testSpec.fromString("abc"));
        assertThat(concat(testSpec.fromString("a"), null, testSpec.fromString("c"))).isNull();
        assertThat(concat(testSpec.fromString("a"), null, null)).isNull();
        assertThat(concat(null, null, null)).isNull();
        assertThat(concat(testSpec.fromString("数据"), testSpec.fromString("砖头")))
                .isEqualTo(testSpec.fromString("数据砖头"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void concatWsTest(TestSpec testSpec) {
        // Returns empty if the separator is null
        assertThat(concatWs(null, (BinaryStringData) null)).isNull();
        assertThat(concatWs(null, testSpec.fromString("a"))).isNull();

        // If separator is null, concatWs should skip all null inputs and never return null.
        BinaryStringData sep = testSpec.fromString("哈哈");
        assertThat(concatWs(sep, testSpec.empty)).isEqualTo(testSpec.empty);
        assertThat(concatWs(sep, testSpec.fromString("ab"))).isEqualTo(testSpec.fromString("ab"));
        assertThat(concatWs(sep, testSpec.fromString("a"), testSpec.fromString("b")))
                .isEqualTo(testSpec.fromString("a哈哈b"));
        assertThat(
                        concatWs(
                                sep,
                                testSpec.fromString("a"),
                                testSpec.fromString("b"),
                                testSpec.fromString("c")))
                .isEqualTo(testSpec.fromString("a哈哈b哈哈c"));
        assertThat(concatWs(sep, testSpec.fromString("a"), null, testSpec.fromString("c")))
                .isEqualTo(testSpec.fromString("a哈哈c"));
        assertThat(concatWs(sep, testSpec.fromString("a"), null, null))
                .isEqualTo(testSpec.fromString("a"));
        assertThat(concatWs(sep, null, null, null)).isEqualTo(testSpec.empty);
        assertThat(concatWs(sep, testSpec.fromString("数据"), testSpec.fromString("砖头")))
                .isEqualTo(testSpec.fromString("数据哈哈砖头"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void contains(TestSpec testSpec) {
        assertThat(testSpec.empty.contains(testSpec.empty)).isTrue();
        assertThat(testSpec.fromString("hello").contains(testSpec.fromString("ello"))).isTrue();
        assertThat(testSpec.fromString("hello").contains(testSpec.fromString("vello"))).isFalse();
        assertThat(testSpec.fromString("hello").contains(testSpec.fromString("hellooo"))).isFalse();
        assertThat(testSpec.fromString("大千世界").contains(testSpec.fromString("千世界"))).isTrue();
        assertThat(testSpec.fromString("大千世界").contains(testSpec.fromString("世千"))).isFalse();
        assertThat(testSpec.fromString("大千世界").contains(testSpec.fromString("大千世界好"))).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void startsWith(TestSpec testSpec) {
        assertThat(testSpec.empty.startsWith(testSpec.empty)).isTrue();
        assertThat(testSpec.fromString("hello").startsWith(testSpec.fromString("hell"))).isTrue();
        assertThat(testSpec.fromString("hello").startsWith(testSpec.fromString("ell"))).isFalse();
        assertThat(testSpec.fromString("hello").startsWith(testSpec.fromString("hellooo")))
                .isFalse();
        assertThat(testSpec.fromString("数据砖头").startsWith(testSpec.fromString("数据"))).isTrue();
        assertThat(testSpec.fromString("大千世界").startsWith(testSpec.fromString("千"))).isFalse();
        assertThat(testSpec.fromString("大千世界").startsWith(testSpec.fromString("大千世界好"))).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void endsWith(TestSpec testSpec) {
        assertThat(testSpec.empty.endsWith(testSpec.empty)).isTrue();
        assertThat(testSpec.fromString("hello").endsWith(testSpec.fromString("ello"))).isTrue();
        assertThat(testSpec.fromString("hello").endsWith(testSpec.fromString("ellov"))).isFalse();
        assertThat(testSpec.fromString("hello").endsWith(testSpec.fromString("hhhello"))).isFalse();
        assertThat(testSpec.fromString("大千世界").endsWith(testSpec.fromString("世界"))).isTrue();
        assertThat(testSpec.fromString("大千世界").endsWith(testSpec.fromString("世"))).isFalse();
        assertThat(testSpec.fromString("数据砖头").endsWith(testSpec.fromString("我的数据砖头"))).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void substring(TestSpec testSpec) {
        assertThat(testSpec.fromString("hello").substring(0, 0)).isEqualTo(testSpec.empty);
        assertThat(testSpec.fromString("hello").substring(1, 3))
                .isEqualTo(testSpec.fromString("el"));
        assertThat(testSpec.fromString("数据砖头").substring(0, 1)).isEqualTo(testSpec.fromString("数"));
        assertThat(testSpec.fromString("数据砖头").substring(1, 3))
                .isEqualTo(testSpec.fromString("据砖"));
        assertThat(testSpec.fromString("数据砖头").substring(3, 5)).isEqualTo(testSpec.fromString("头"));
        assertThat(testSpec.fromString("ߵ梷").substring(0, 2)).isEqualTo(testSpec.fromString("ߵ梷"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void trims(TestSpec testSpec) {
        assertThat(testSpec.fromString("1").trim()).isEqualTo(testSpec.fromString("1"));

        assertThat(testSpec.fromString("  hello ").trim()).isEqualTo(testSpec.fromString("hello"));
        assertThat(trimLeft(testSpec.fromString("  hello ")))
                .isEqualTo(testSpec.fromString("hello "));
        assertThat(trimRight(testSpec.fromString("  hello ")))
                .isEqualTo(testSpec.fromString("  hello"));

        assertThat(trim(testSpec.fromString("  hello "), false, false, testSpec.fromString(" ")))
                .isEqualTo(testSpec.fromString("  hello "));
        assertThat(trim(testSpec.fromString("  hello "), true, true, testSpec.fromString(" ")))
                .isEqualTo(testSpec.fromString("hello"));
        assertThat(trim(testSpec.fromString("  hello "), true, false, testSpec.fromString(" ")))
                .isEqualTo(testSpec.fromString("hello "));
        assertThat(trim(testSpec.fromString("  hello "), false, true, testSpec.fromString(" ")))
                .isEqualTo(testSpec.fromString("  hello"));
        assertThat(trim(testSpec.fromString("xxxhellox"), true, true, testSpec.fromString("x")))
                .isEqualTo(testSpec.fromString("hello"));

        assertThat(trim(testSpec.fromString("xxxhellox"), testSpec.fromString("xoh")))
                .isEqualTo(testSpec.fromString("ell"));

        assertThat(trimLeft(testSpec.fromString("xxxhellox"), testSpec.fromString("xoh")))
                .isEqualTo(testSpec.fromString("ellox"));

        assertThat(trimRight(testSpec.fromString("xxxhellox"), testSpec.fromString("xoh")))
                .isEqualTo(testSpec.fromString("xxxhell"));

        assertThat(testSpec.empty.trim()).isEqualTo(testSpec.empty);
        assertThat(testSpec.fromString("  ").trim()).isEqualTo(testSpec.empty);
        assertThat(trimLeft(testSpec.fromString("  "))).isEqualTo(testSpec.empty);
        assertThat(trimRight(testSpec.fromString("  "))).isEqualTo(testSpec.empty);

        assertThat(testSpec.fromString("  数据砖头 ").trim()).isEqualTo(testSpec.fromString("数据砖头"));
        assertThat(trimLeft(testSpec.fromString("  数据砖头 ")))
                .isEqualTo(testSpec.fromString("数据砖头 "));
        assertThat(trimRight(testSpec.fromString("  数据砖头 ")))
                .isEqualTo(testSpec.fromString("  数据砖头"));

        assertThat(testSpec.fromString("数据砖头").trim()).isEqualTo(testSpec.fromString("数据砖头"));
        assertThat(trimLeft(testSpec.fromString("数据砖头"))).isEqualTo(testSpec.fromString("数据砖头"));
        assertThat(trimRight(testSpec.fromString("数据砖头"))).isEqualTo(testSpec.fromString("数据砖头"));

        assertThat(trim(testSpec.fromString("年年岁岁, 岁岁年年"), testSpec.fromString("年岁 ")))
                .isEqualTo(testSpec.fromString(","));
        assertThat(trimLeft(testSpec.fromString("年年岁岁, 岁岁年年"), testSpec.fromString("年岁 ")))
                .isEqualTo(testSpec.fromString(", 岁岁年年"));
        assertThat(trimRight(testSpec.fromString("年年岁岁, 岁岁年年"), testSpec.fromString("年岁 ")))
                .isEqualTo(testSpec.fromString("年年岁岁,"));

        char[] charsLessThan0x20 = new char[10];
        Arrays.fill(charsLessThan0x20, (char) (' ' - 1));
        String stringStartingWithSpace =
                new String(charsLessThan0x20) + "hello" + new String(charsLessThan0x20);
        assertThat(testSpec.fromString(stringStartingWithSpace).trim())
                .isEqualTo(testSpec.fromString(stringStartingWithSpace));
        assertThat(trimLeft(testSpec.fromString(stringStartingWithSpace)))
                .isEqualTo(testSpec.fromString(stringStartingWithSpace));
        assertThat(trimRight(testSpec.fromString(stringStartingWithSpace)))
                .isEqualTo(testSpec.fromString(stringStartingWithSpace));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testSqlSubstring(TestSpec testSpec) {
        assertThat(substringSQL(testSpec.fromString("hello"), 2))
                .isEqualTo(testSpec.fromString("ello"));
        assertThat(substringSQL(testSpec.fromString("hello"), 2, 3))
                .isEqualTo(testSpec.fromString("ell"));
        assertThat(substringSQL(testSpec.empty, 2, 3)).isEqualTo(testSpec.empty);
        assertThat(substringSQL(testSpec.fromString("hello"), 0, -1)).isNull();
        assertThat(substringSQL(testSpec.fromString("hello"), 10)).isEqualTo(testSpec.empty);
        assertThat(substringSQL(testSpec.fromString("hello"), 0, 3))
                .isEqualTo(testSpec.fromString("hel"));
        assertThat(substringSQL(testSpec.fromString("hello"), -2, 3))
                .isEqualTo(testSpec.fromString("lo"));
        assertThat(substringSQL(testSpec.fromString("hello"), -100, 3)).isEqualTo(testSpec.empty);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void reverseTest(TestSpec testSpec) {
        assertThat(reverse(testSpec.fromString("hello"))).isEqualTo(testSpec.fromString("olleh"));
        assertThat(reverse(testSpec.fromString("中国"))).isEqualTo(testSpec.fromString("国中"));
        assertThat(reverse(testSpec.fromString("hello, 中国")))
                .isEqualTo(testSpec.fromString("国中 ,olleh"));
        assertThat(reverse(testSpec.empty)).isEqualTo(testSpec.empty);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void indexOf(TestSpec testSpec) {
        assertThat(testSpec.empty.indexOf(testSpec.empty, 0)).isEqualTo(0);
        assertThat(testSpec.empty.indexOf(testSpec.fromString("l"), 0)).isEqualTo(-1);
        assertThat(testSpec.fromString("hello").indexOf(testSpec.empty, 0)).isEqualTo(0);
        assertThat(testSpec.fromString("hello").indexOf(testSpec.fromString("l"), 0)).isEqualTo(2);
        assertThat(testSpec.fromString("hello").indexOf(testSpec.fromString("l"), 3)).isEqualTo(3);
        assertThat(testSpec.fromString("hello").indexOf(testSpec.fromString("a"), 0)).isEqualTo(-1);
        assertThat(testSpec.fromString("hello").indexOf(testSpec.fromString("ll"), 0)).isEqualTo(2);
        assertThat(testSpec.fromString("hello").indexOf(testSpec.fromString("ll"), 4))
                .isEqualTo(-1);
        assertThat(testSpec.fromString("数据砖头").indexOf(testSpec.fromString("据砖"), 0)).isEqualTo(1);
        assertThat(testSpec.fromString("数据砖头").indexOf(testSpec.fromString("数"), 3)).isEqualTo(-1);
        assertThat(testSpec.fromString("数据砖头").indexOf(testSpec.fromString("数"), 0)).isEqualTo(0);
        assertThat(testSpec.fromString("数据砖头").indexOf(testSpec.fromString("头"), 0)).isEqualTo(3);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testToNumeric(TestSpec testSpec) {
        // Test to integer.
        assertThat(toByte(testSpec.fromString("123"))).isEqualTo(Byte.parseByte("123"));
        assertThat(toByte(testSpec.fromString("+123"))).isEqualTo(Byte.parseByte("123"));
        assertThat(toByte(testSpec.fromString("-123"))).isEqualTo(Byte.parseByte("-123"));

        assertThat(toShort(testSpec.fromString("123"))).isEqualTo(Short.parseShort("123"));
        assertThat(toShort(testSpec.fromString("+123"))).isEqualTo(Short.parseShort("123"));
        assertThat(toShort(testSpec.fromString("-123"))).isEqualTo(Short.parseShort("-123"));

        assertThat(toInt(testSpec.fromString("123"))).isEqualTo(Integer.parseInt("123"));
        assertThat(toInt(testSpec.fromString("+123"))).isEqualTo(Integer.parseInt("123"));
        assertThat(toInt(testSpec.fromString("-123"))).isEqualTo(Integer.parseInt("-123"));

        assertThat(toLong(testSpec.fromString("1234567890")))
                .isEqualTo(Long.parseLong("1234567890"));
        assertThat(toLong(testSpec.fromString("+1234567890")))
                .isEqualTo(Long.parseLong("+1234567890"));
        assertThat(toLong(testSpec.fromString("-1234567890")))
                .isEqualTo(Long.parseLong("-1234567890"));

        // Test decimal string to integer.
        assertThat(toInt(testSpec.fromString("123.456789"))).isEqualTo(Integer.parseInt("123"));
        assertThat(toLong(testSpec.fromString("123.456789"))).isEqualTo(Long.parseLong("123"));

        // Test negative cases.
        assertThatThrownBy(() -> toInt(testSpec.fromString("1a3.456789")))
                .isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> toInt(testSpec.fromString("123.a56789")))
                .isInstanceOf(NumberFormatException.class);

        // Test composite in BinaryRowData.
        BinaryRowData row = new BinaryRowData(20);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, testSpec.fromString("1"));
        writer.writeString(1, testSpec.fromString("123"));
        writer.writeString(2, testSpec.fromString("12345"));
        writer.writeString(3, testSpec.fromString("123456789"));
        writer.complete();

        assertThat(toByte(((BinaryStringData) row.getString(0)))).isEqualTo(Byte.parseByte("1"));
        assertThat(toShort(((BinaryStringData) row.getString(1))))
                .isEqualTo(Short.parseShort("123"));
        assertThat(toInt(((BinaryStringData) row.getString(2))))
                .isEqualTo(Integer.parseInt("12345"));
        assertThat(toLong(((BinaryStringData) row.getString(3))))
                .isEqualTo(Long.parseLong("123456789"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testToUpperLowerCase(TestSpec testSpec) {
        assertThat(testSpec.fromString("我是中国人").toLowerCase())
                .isEqualTo(testSpec.fromString("我是中国人"));
        assertThat(testSpec.fromString("我是中国人").toUpperCase())
                .isEqualTo(testSpec.fromString("我是中国人"));

        assertThat(testSpec.fromString("aBcDeFg").toLowerCase())
                .isEqualTo(testSpec.fromString("abcdefg"));
        assertThat(testSpec.fromString("aBcDeFg").toUpperCase())
                .isEqualTo(testSpec.fromString("ABCDEFG"));

        assertThat(testSpec.fromString("!@#$%^*").toLowerCase())
                .isEqualTo(testSpec.fromString("!@#$%^*"))
                .isEqualTo(testSpec.fromString("!@#$%^*"));
        // Test composite in BinaryRowData.
        BinaryRowData row = new BinaryRowData(20);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, testSpec.fromString("a"));
        writer.writeString(1, testSpec.fromString("我是中国人"));
        writer.writeString(3, testSpec.fromString("aBcDeFg"));
        writer.writeString(5, testSpec.fromString("!@#$%^*"));
        writer.complete();

        assertThat(((BinaryStringData) row.getString(0)).toUpperCase())
                .isEqualTo(testSpec.fromString("A"));
        assertThat(((BinaryStringData) row.getString(1)).toUpperCase())
                .isEqualTo(testSpec.fromString("我是中国人"));
        assertThat(((BinaryStringData) row.getString(1)).toLowerCase())
                .isEqualTo(testSpec.fromString("我是中国人"));
        assertThat(((BinaryStringData) row.getString(3)).toUpperCase())
                .isEqualTo(testSpec.fromString("ABCDEFG"));
        assertThat(((BinaryStringData) row.getString(3)).toLowerCase())
                .isEqualTo(testSpec.fromString("abcdefg"));
        assertThat(((BinaryStringData) row.getString(5)).toUpperCase())
                .isEqualTo(testSpec.fromString("!@#$%^*"));
        assertThat(((BinaryStringData) row.getString(5)).toLowerCase())
                .isEqualTo(testSpec.fromString("!@#$%^*"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testToDecimal(TestSpec testSpec) {
        class DecimalTestData {
            private String str;
            private int precision, scale;

            private DecimalTestData(String str, int precision, int scale) {
                this.str = str;
                this.precision = precision;
                this.scale = scale;
            }
        }

        DecimalTestData[] data = {
            new DecimalTestData("12.345", 5, 3),
            new DecimalTestData("-12.345", 5, 3),
            new DecimalTestData("+12345", 5, 0),
            new DecimalTestData("-12345", 5, 0),
            new DecimalTestData("12345.", 5, 0),
            new DecimalTestData("-12345.", 5, 0),
            new DecimalTestData(".12345", 5, 5),
            new DecimalTestData("-.12345", 5, 5),
            new DecimalTestData("+12.345E3", 5, 0),
            new DecimalTestData("-12.345e3", 5, 0),
            new DecimalTestData("12.345e-3", 6, 6),
            new DecimalTestData("-12.345E-3", 6, 6),
            new DecimalTestData("12345E3", 8, 0),
            new DecimalTestData("-12345e3", 8, 0),
            new DecimalTestData("12345e-3", 5, 3),
            new DecimalTestData("-12345E-3", 5, 3),
            new DecimalTestData("+.12345E3", 5, 2),
            new DecimalTestData("-.12345e3", 5, 2),
            new DecimalTestData(".12345e-3", 8, 8),
            new DecimalTestData("-.12345E-3", 8, 8),
            new DecimalTestData("1234512345.1234", 18, 8),
            new DecimalTestData("-1234512345.1234", 18, 8),
            new DecimalTestData("1234512345.1234", 12, 2),
            new DecimalTestData("-1234512345.1234", 12, 2),
            new DecimalTestData("1234512345.1299", 12, 2),
            new DecimalTestData("-1234512345.1299", 12, 2),
            new DecimalTestData("999999999999999999", 18, 0),
            new DecimalTestData("1234512345.1234512345", 20, 10),
            new DecimalTestData("-1234512345.1234512345", 20, 10),
            new DecimalTestData("1234512345.1234512345", 15, 5),
            new DecimalTestData("-1234512345.1234512345", 15, 5),
            new DecimalTestData("12345123451234512345E-10", 20, 10),
            new DecimalTestData("-12345123451234512345E-10", 20, 10),
            new DecimalTestData("12345123451234512345E-10", 15, 5),
            new DecimalTestData("-12345123451234512345E-10", 15, 5),
            new DecimalTestData("999999999999999999999", 21, 0),
            new DecimalTestData("-999999999999999999999", 21, 0),
            new DecimalTestData("0.00000000000000000000123456789123456789", 38, 38),
            new DecimalTestData("-0.00000000000000000000123456789123456789", 38, 38),
            new DecimalTestData("0.00000000000000000000123456789123456789", 29, 29),
            new DecimalTestData("-0.00000000000000000000123456789123456789", 29, 29),
            new DecimalTestData("123456789123E-27", 18, 18),
            new DecimalTestData("-123456789123E-27", 18, 18),
            new DecimalTestData("123456789999E-27", 18, 18),
            new DecimalTestData("-123456789999E-27", 18, 18),
            new DecimalTestData("123456789123456789E-36", 18, 18),
            new DecimalTestData("-123456789123456789E-36", 18, 18),
            new DecimalTestData("123456789999999999E-36", 18, 18),
            new DecimalTestData("-123456789999999999E-36", 18, 18)
        };

        for (DecimalTestData d : data) {
            assertThat(toDecimal(testSpec.fromString(d.str), d.precision, d.scale))
                    .isEqualTo(
                            DecimalData.fromBigDecimal(
                                    new BigDecimal(d.str), d.precision, d.scale));
        }

        BinaryRowData row = new BinaryRowData(data.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < data.length; i++) {
            writer.writeString(i, testSpec.fromString(data[i].str));
        }
        writer.complete();
        for (int i = 0; i < data.length; i++) {
            DecimalTestData d = data[i];
            assertThat(toDecimal((BinaryStringData) row.getString(i), d.precision, d.scale))
                    .isEqualTo(
                            DecimalData.fromBigDecimal(
                                    new BigDecimal(d.str), d.precision, d.scale));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testEmptyString(TestSpec testSpec) {
        BinaryStringData str2 = testSpec.fromString("hahahahah");
        BinaryStringData str3;
        {
            MemorySegment[] segments = new MemorySegment[2];
            segments[0] = MemorySegmentFactory.wrap(new byte[10]);
            segments[1] = MemorySegmentFactory.wrap(new byte[10]);
            str3 = BinaryStringData.fromAddress(segments, 15, 0);
        }

        assertThat(BinaryStringData.EMPTY_UTF8).isLessThan(str2);
        assertThat(str2).isGreaterThan(BinaryStringData.EMPTY_UTF8);

        assertThat(BinaryStringData.EMPTY_UTF8).isEqualByComparingTo(str3);
        assertThat(str3).isEqualByComparingTo(BinaryStringData.EMPTY_UTF8);

        assertThat(str2).isNotEqualTo(BinaryStringData.EMPTY_UTF8);
        assertThat(BinaryStringData.EMPTY_UTF8).isNotEqualTo(str2);

        assertThat(str3).isEqualTo(BinaryStringData.EMPTY_UTF8);
        assertThat(BinaryStringData.EMPTY_UTF8).isEqualTo(str3);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testEncodeWithIllegalCharacter(TestSpec mode) throws UnsupportedEncodingException {

        // Tis char array has some illegal character, such as 55357
        // the jdk ignores theses character and cast them to '?'
        // which StringUtf8Utils'encodeUTF8 should follow
        char[] chars =
                new char[] {
                    20122, 40635, 124, 38271, 34966, 124, 36830, 34915, 35033, 124, 55357, 124,
                    56407
                };

        String str = new String(chars);

        assertThat(StringUtf8Utils.encodeUTF8(str)).isEqualTo(str.getBytes("UTF-8"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testKeyValue(TestSpec testSpec) {
        assertThat(
                        keyValue(
                                testSpec.fromString("k1:v1|k2:v2"),
                                testSpec.fromString("|").byteAt(0),
                                testSpec.fromString(":").byteAt(0),
                                testSpec.fromString("k3")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1:v1|k2:v2|"),
                                testSpec.fromString("|").byteAt(0),
                                testSpec.fromString(":").byteAt(0),
                                testSpec.fromString("k3")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("|k1:v1|k2:v2|"),
                                testSpec.fromString("|").byteAt(0),
                                testSpec.fromString(":").byteAt(0),
                                testSpec.fromString("k3")))
                .isNull();
        String tab = StringEscapeUtils.unescapeJava("\t");
        assertThat(
                        keyValue(
                                testSpec.fromString("k1:v1" + tab + "k2:v2"),
                                testSpec.fromString("\t").byteAt(0),
                                testSpec.fromString(":").byteAt(0),
                                testSpec.fromString("k2")))
                .isEqualTo(testSpec.fromString("v2"));
        assertThat(
                        keyValue(
                                testSpec.fromString("k1:v1|k2:v2"),
                                testSpec.fromString("|").byteAt(0),
                                testSpec.fromString(":").byteAt(0),
                                null))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1=v1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k2")))
                .isEqualTo(testSpec.fromString("v2"));
        assertThat(
                        keyValue(
                                testSpec.fromString("|k1=v1|k2=v2|"),
                                testSpec.fromString("|").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k2")))
                .isEqualTo(testSpec.fromString("v2"));
        assertThat(
                        keyValue(
                                testSpec.fromString("k1=v1||k2=v2"),
                                testSpec.fromString("|").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k2")))
                .isEqualTo(testSpec.fromString("v2"));
        assertThat(
                        keyValue(
                                testSpec.fromString("k1=v1;k2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k2")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k1")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k=1=v1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k=")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1==v1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k1")))
                .isEqualTo(testSpec.fromString("=v1"));
        assertThat(
                        keyValue(
                                testSpec.fromString("k1==v1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k1=")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1=v1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k1=")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1k1=v1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k1")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1=v1;k2=v2"),
                                testSpec.fromString(";").byteAt(0),
                                testSpec.fromString("=").byteAt(0),
                                testSpec.fromString("k1k1k1k1k1k1k1k1k1k1")))
                .isNull();
        assertThat(
                        keyValue(
                                testSpec.fromString("k1:v||k2:v2"),
                                testSpec.fromString("|").byteAt(0),
                                testSpec.fromString(":").byteAt(0),
                                testSpec.fromString("k2")))
                .isEqualTo(testSpec.fromString("v2"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testDecodeWithIllegalUtf8Bytes(TestSpec mode) throws UnsupportedEncodingException {

        // illegal utf-8 bytes
        byte[] bytes =
                new byte[] {
                    (byte) 20122,
                    (byte) 40635,
                    124,
                    (byte) 38271,
                    (byte) 34966,
                    124,
                    (byte) 36830,
                    (byte) 34915,
                    (byte) 35033,
                    124,
                    (byte) 55357,
                    124,
                    (byte) 56407
                };

        String str = new String(bytes, StandardCharsets.UTF_8);
        assertThat(StringUtf8Utils.decodeUTF8(bytes, 0, bytes.length)).isEqualTo(str);
        assertThat(StringUtf8Utils.decodeUTF8(MemorySegmentFactory.wrap(bytes), 0, bytes.length))
                .isEqualTo(str);

        byte[] newBytes = new byte[bytes.length + 5];
        System.arraycopy(bytes, 0, newBytes, 5, bytes.length);
        assertThat(StringUtf8Utils.decodeUTF8(MemorySegmentFactory.wrap(newBytes), 5, bytes.length))
                .isEqualTo(str);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void skipWrongFirstByte(TestSpec mode) {
        int[] wrongFirstBytes = {
            0x80,
            0x9F,
            0xBF, // Skip Continuation bytes
            0xC0,
            0xC2, // 0xC0..0xC1 - disallowed in UTF-8
            // 0xF5..0xFF - disallowed in UTF-8
            0xF5,
            0xF6,
            0xF7,
            0xF8,
            0xF9,
            0xFA,
            0xFB,
            0xFC,
            0xFD,
            0xFE,
            0xFF
        };
        byte[] c = new byte[1];

        for (int wrongFirstByte : wrongFirstBytes) {
            c[0] = (byte) wrongFirstByte;
            assertThat(fromBytes(c).numChars()).isEqualTo(1);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testSplit(TestSpec mode) {
        assertThat(splitByWholeSeparatorPreserveAllTokens(mode.fromString(""), mode.fromString("")))
                .isEqualTo(EMPTY_STRING_ARRAY);
        assertThat(splitByWholeSeparatorPreserveAllTokens(mode.fromString("ab de fg"), null))
                .isEqualTo(
                        new BinaryStringData[] {
                            mode.fromString("ab"), mode.fromString("de"), mode.fromString("fg")
                        });
        assertThat(splitByWholeSeparatorPreserveAllTokens(mode.fromString("ab   de fg"), null))
                .isEqualTo(
                        new BinaryStringData[] {
                            mode.fromString("ab"),
                            mode.fromString(""),
                            mode.fromString(""),
                            mode.fromString("de"),
                            mode.fromString("fg")
                        });
        assertThat(
                        splitByWholeSeparatorPreserveAllTokens(
                                mode.fromString("ab:cd:ef"), mode.fromString(":")))
                .isEqualTo(
                        new BinaryStringData[] {
                            mode.fromString("ab"), mode.fromString("cd"), mode.fromString("ef")
                        });
        assertThat(
                        splitByWholeSeparatorPreserveAllTokens(
                                mode.fromString("ab-!-cd-!-ef"), mode.fromString("-!-")))
                .isEqualTo(
                        new BinaryStringData[] {
                            mode.fromString("ab"), mode.fromString("cd"), mode.fromString("ef")
                        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getVarSeg")
    void testLazy(TestSpec mode) {
        String javaStr = "haha";
        BinaryStringData str = BinaryStringData.fromString(javaStr);
        str.ensureMaterialized();

        // check reference same.
        assertThat(javaStr).isSameAs(str.toString());
    }
}
