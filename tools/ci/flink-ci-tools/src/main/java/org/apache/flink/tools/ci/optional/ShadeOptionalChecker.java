/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tools.ci.optional;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.tools.ci.utils.dependency.DependencyParser;
import org.apache.flink.tools.ci.utils.shade.ShadeParser;
import org.apache.flink.tools.ci.utils.shared.Dependency;
import org.apache.flink.tools.ci.utils.shared.DependencyTree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Verifies that all dependencies bundled with the shade-plugin are marked as optional in the pom.
 * This ensures compatibility with later maven versions and in general simplifies dependency
 * management as transitivity is no longer dependent on the shade-plugin.
 */
public class ShadeOptionalChecker {
    private static final Logger LOG = LoggerFactory.getLogger(ShadeOptionalChecker.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println(
                    "Usage: ShadeOptionalChecker <pathShadeBuildOutput> <pathMavenDependencyOutput>");
            System.exit(1);
        }

        final Path shadeOutputPath = Paths.get(args[0]);
        final Path dependencyOutputPath = Paths.get(args[1]);

        final Map<String, Set<Dependency>> bundledDependenciesByModule =
                ShadeParser.parseShadeOutput(shadeOutputPath);
        final Map<String, DependencyTree> dependenciesByModule =
                DependencyParser.parseDependencyTreeOutput(dependencyOutputPath);

        final Map<String, Set<Dependency>> violations =
                checkOptionalFlags(bundledDependenciesByModule, dependenciesByModule);

        if (!violations.isEmpty()) {
            LOG.error(
                    "{} modules bundle in total {} dependencies without them being marked as optional in the pom.",
                    violations.keySet().size(),
                    violations.size());
            LOG.error(
                    "\tIn order for shading to properly work within Flink we require all bundled dependencies to be marked as optional in the pom.");
            LOG.error(
                    "\tFor verification purposes we require the dependency tree from the dependency-plugin to show the dependency as either:");
            LOG.error("\t\ta) an optional dependency,");
            LOG.error("\t\tb) a transitive dependency of another optional dependency.");
            LOG.error(
                    "\tIn most cases adding '<optional>${flink.markBundledAsOptional}</optional>' to the bundled dependency is sufficient.");
            LOG.error(
                    "\tThere are some edge cases where a transitive dependency might be associated with the \"wrong\" dependency in the tree, for example if a test dependency also requires it.");
            LOG.error(
                    "\tIn such cases you need to adjust the poms so that the dependency shows up in the right spot. This may require adding an explicit dependency (Management) entry, excluding dependencies, or at times even reordering dependencies in the pom.");
            LOG.error(
                    "\tSee the Dependencies page in the wiki for details: https://cwiki.apache.org/confluence/display/FLINK/Dependencies");

            for (String moduleWithViolations : violations.keySet()) {
                final Collection<Dependency> dependencyViolations =
                        violations.get(moduleWithViolations);
                LOG.error(
                        "\tModule {} ({} violation{}):",
                        moduleWithViolations,
                        dependencyViolations.size(),
                        dependencyViolations.size() == 1 ? "" : "s");
                for (Dependency dependencyViolation : dependencyViolations) {
                    LOG.error("\t\t{}", dependencyViolation);
                }
            }

            System.exit(1);
        }
    }

    private static Map<String, Set<Dependency>> checkOptionalFlags(
            Map<String, Set<Dependency>> bundledDependenciesByModule,
            Map<String, DependencyTree> dependenciesByModule) {

        final Map<String, Set<Dependency>> allViolations = new HashMap<>();

        for (String module : bundledDependenciesByModule.keySet()) {
            LOG.debug("Checking module '{}'.", module);
            if (!dependenciesByModule.containsKey(module)) {
                throw new IllegalStateException(
                        String.format(
                                "Module %s listed by shade-plugin, but not dependency-plugin.",
                                module));
            }

            final Collection<Dependency> bundledDependencies =
                    bundledDependenciesByModule.get(module);
            final DependencyTree dependencyTree = dependenciesByModule.get(module);

            final Set<Dependency> violations =
                    checkOptionalFlags(module, bundledDependencies, dependencyTree);

            if (violations.isEmpty()) {
                LOG.info("OK: {}", module);
            } else {
                allViolations.put(module, violations);
            }
        }

        return allViolations;
    }

    @VisibleForTesting
    static Set<Dependency> checkOptionalFlags(
            String module,
            Collection<Dependency> bundledDependencies,
            DependencyTree dependencyTree) {

        bundledDependencies =
                bundledDependencies.stream()
                        // force-shading isn't relevant for this check but breaks some shortcuts
                        .filter(
                                dependency ->
                                        !dependency
                                                .getArtifactId()
                                                .equals("flink-shaded-force-shading"))
                        .collect(Collectors.toSet());

        final Set<Dependency> violations = new HashSet<>();

        if (bundledDependencies.isEmpty()) {
            LOG.debug("\tModule is not bundling any dependencies.");
            return violations;
        }

        // If a module has no transitive dependencies we can shortcut the optional flag checks as
        // we will not require additional flags in any case.
        // This reduces noise on CI.
        // It also avoids some edge-cases; since a dependency can only occur once in the dependency
        // tree (on the shortest path to said dependency) it can happen that a compile dependency
        // is shown as a transitive dependency of a test dependency.
        final List<Dependency> directTransitiveDependencies =
                dependencyTree.getDirectDependencies().stream()
                        .filter(
                                dependency ->
                                        !(dependency.isOptional().orElse(false)
                                                || "provided"
                                                        .equals(dependency.getScope().orElse(null))
                                                || "test".equals(dependency.getScope().orElse(null))
                                                || "flink-shaded-force-shading"
                                                        .equals(dependency.getArtifactId())
                                                || "jsr305".equals(dependency.getArtifactId())
                                                || "slf4j-api".equals(dependency.getArtifactId())))
                        .collect(Collectors.toList());

        if (directTransitiveDependencies.isEmpty()) {
            LOG.debug(
                    "Skipping deep-check of module {} because all direct dependencies are not transitive.",
                    module);
            return violations;
        }
        LOG.debug(
                "Running deep-check of module {} because there are direct dependencies that are transitive: {}",
                module,
                directTransitiveDependencies);

        for (Dependency bundledDependency : bundledDependencies) {
            LOG.debug("\tChecking dependency '{}'.", bundledDependency);

            final List<Dependency> dependencyPath = dependencyTree.getPathTo(bundledDependency);

            final boolean isOptional =
                    dependencyPath.stream().anyMatch(parent -> parent.isOptional().orElse(false));

            if (!isOptional) {
                violations.add(bundledDependency);
            }
        }

        return violations;
    }
}
