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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.utils.SimplePath;
import org.apache.flink.table.utils.SimplePath.Step;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;

import org.apache.flink.shaded.com.jayway.jsonpath.Configuration;
import org.apache.flink.shaded.com.jayway.jsonpath.JsonPath;
import org.apache.flink.shaded.com.jayway.jsonpath.PathNotFoundException;
import org.apache.flink.shaded.com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import org.apache.flink.shaded.com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.NullNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that every path {@link SimplePath} accepts is also a definite path of the Jayway JsonPath
 * library that the JSON functions use, and that both find the same value in the same document.
 */
class SimplePathJaywayConformanceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Configuration JAYWAY =
            Configuration.builder()
                    .jsonProvider(new JacksonJsonProvider())
                    .mappingProvider(new JacksonMappingProvider())
                    .build();

    private static final List<String> NAMES =
            List.of(
                    "a",
                    "b",
                    "arr",
                    "n",
                    "s",
                    "a b",
                    "a.b",
                    "it's",
                    "say \"hi\"",
                    "a\\b",
                    "",
                    "0",
                    "@pages",
                    "$",
                    "#",
                    "a-b:c?,!",
                    "é",
                    "a]b[c",
                    "*",
                    "length",
                    "missing");

    private static final List<String> DOCUMENTS =
            List.of(
                    "{\"a\":{\"b\":1,\"arr\":[10,{\"s\":\"x\"}],\"n\":null},"
                            + "\"arr\":[[1,2],[3],{\"a\":\"in array\"}],\"n\":null,\"s\":\"str\","
                            + "\"a b\":2,\"a.b\":3,\"it's\":4,\"say \\\"hi\\\"\":5,\"a\\\\b\":6,"
                            + "\"\":7,\"0\":8,\"@pages\":9,\"$\":10,\"#\":11,\"a-b:c?,!\":12,"
                            + "\"é\":13,\"a]b[c\":14,\"*\":15,\"length\":16}",
                    "[{\"a\":1,\"b\":[true,false]},[2,3],null,\"s\",{\"0\":\"zero\"}]",
                    "42");

    // A name that both parsers read the same after a dot.
    private static final Pattern DOT_NAME = Pattern.compile("[A-Za-z0-9@#$:,?!é-]+");

    private static final String[] TOKENS = {
        "$",
        ".",
        "[",
        "]",
        "'",
        "\"",
        "\\",
        "0",
        "1",
        "12",
        "a",
        "b",
        "arr",
        "-",
        "@",
        "#",
        ":",
        ",",
        "?",
        "*",
        "(",
        ")",
        " ",
        "é",
        ".a",
        ".arr",
        "['a b']",
        "[0]",
        "[1]",
        "[\"a.b\"]",
        "['it\\'s']",
        "[-1]",
        "..",
        "length()",
        "['a','b']",
        "[0:1]"
    };

    // Paths that the generators below are unlikely to produce.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"$.arr[007]", "$[2147483647]", "$.a.b   "})
    void testCuratedPaths(final String path) {
        assertConforms(path);
    }

    @Test
    void testRandomStrings() {
        final Random random = new Random(42);
        int accepted = 0;
        for (int i = 0; i < 50_000; i++) {
            final StringBuilder path = new StringBuilder(random.nextInt(10) == 0 ? "" : "$");
            final int tokens = random.nextInt(6);
            for (int t = 0; t < tokens; t++) {
                path.append(TOKENS[random.nextInt(TOKENS.length)]);
            }
            if (isAccepted(path.toString())) {
                accepted++;
                assertConforms(path.toString());
            }
        }
        // Guards against a generator that only produces rejected paths.
        assertThat(accepted).isGreaterThan(1_000);
    }

    @Test
    void testRandomSteps() {
        final Random random = new Random(7);
        for (int i = 0; i < 10_000; i++) {
            final List<Step> steps = new ArrayList<>();
            final StringBuilder path = new StringBuilder("$");
            final int count = random.nextInt(5);
            for (int s = 0; s < count; s++) {
                if (random.nextInt(4) == 0) {
                    final int index = random.nextInt(4);
                    steps.add(Step.index(index));
                    path.append('[').append(index).append(']');
                } else {
                    final String name = NAMES.get(random.nextInt(NAMES.size()));
                    steps.add(Step.field(name));
                    path.append(render(name, random));
                }
            }

            assertThat(SimplePath.parse(path.toString()).getSteps())
                    .as(path.toString())
                    .isEqualTo(steps);
            assertConforms(path.toString());
        }
    }

    private static String render(final String name, final Random random) {
        if (DOT_NAME.matcher(name).matches() && random.nextBoolean()) {
            return "." + name;
        }
        final char quote = random.nextBoolean() ? '\'' : '"';
        final char other = quote == '\'' ? '"' : '\'';
        final StringBuilder rendered = new StringBuilder("[").append(quote);
        for (final char c : name.toCharArray()) {
            if (c == '\\' || c == quote || (c == other && random.nextBoolean())) {
                rendered.append('\\');
            }
            rendered.append(c);
        }
        return rendered.append(quote).append(']').toString();
    }

    private static boolean isAccepted(final String path) {
        try {
            SimplePath.parse(path);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static void assertConforms(final String path) {
        final SimplePath simplePath = SimplePath.parse(path);
        final JsonPath jsonPath = JsonPath.compile(path);
        assertThat(jsonPath.isDefinite()).as("Jayway reads '%s' as definite", path).isTrue();
        for (final String document : DOCUMENTS) {
            assertThat(readWithSimplePath(simplePath, document))
                    .as("'%s' in %s", path, document)
                    .isEqualTo(readWithJayway(jsonPath, document));
        }
    }

    private static Optional<JsonNode> readWithSimplePath(
            final SimplePath path, final String document) {
        try {
            final Variant variant = BinaryVariantInternalBuilder.parseJson(document, false);
            final Variant value = VariantGetFunction.navigate(variant, path);
            return value == null ? Optional.empty() : Optional.of(MAPPER.readTree(value.toJson()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Optional<JsonNode> readWithJayway(final JsonPath path, final String document) {
        try {
            final Object value = JsonPath.using(JAYWAY).parse(document).read(path);
            return Optional.of(value == null ? NullNode.getInstance() : MAPPER.valueToTree(value));
        } catch (PathNotFoundException e) {
            return Optional.empty();
        }
    }
}
