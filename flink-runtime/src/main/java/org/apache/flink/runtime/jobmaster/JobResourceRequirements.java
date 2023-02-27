/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Information about the parallelism of job vertices. */
public class JobResourceRequirements implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String JOB_RESOURCE_REQUIREMENTS_KEY = "$job-resource-requirements";

    private static final JobResourceRequirements EMPTY =
            new JobResourceRequirements(Collections.emptyMap());

    /**
     * Write {@link JobResourceRequirements resource requirements} into the configuration of a given
     * {@link JobGraph}.
     *
     * @param jobGraph job graph to write requirements to
     * @param jobResourceRequirements resource requirements to write
     * @throws IOException in case we're not able to serialize requirements into the configuration
     */
    public static void writeToJobGraph(
            JobGraph jobGraph, JobResourceRequirements jobResourceRequirements) throws IOException {
        InstantiationUtil.writeObjectToConfig(
                jobResourceRequirements,
                jobGraph.getJobConfiguration(),
                JOB_RESOURCE_REQUIREMENTS_KEY);
    }

    /**
     * Read {@link JobResourceRequirements resource requirements} from the configuration of a given
     * {@link JobGraph}.
     *
     * @param jobGraph job graph to read requirements from
     * @throws IOException in case we're not able to deserialize requirements from the configuration
     * @throws ClassNotFoundException in case some deserialized classes are missing on the classpath
     */
    public static Optional<JobResourceRequirements> readFromJobGraph(JobGraph jobGraph)
            throws IOException, ClassNotFoundException {
        return Optional.ofNullable(
                InstantiationUtil.readObjectFromConfig(
                        jobGraph.getJobConfiguration(),
                        JOB_RESOURCE_REQUIREMENTS_KEY,
                        JobResourceRequirements.class.getClassLoader()));
    }

    /**
     * This method validates that the new job vertex parallelisms are less or equal to the max
     * parallelism. Moreover, it validates that there are no unknown job vertex ids and that we're
     * not missing any.
     *
     * @param jobResourceRequirements contains the new resources requirements for the job vertices
     * @param maxParallelismPerVertex allows us to look up maximum possible parallelism for a job
     *     vertex
     * @return a list of validation errors
     */
    public static List<String> validate(
            JobResourceRequirements jobResourceRequirements,
            Map<JobVertexID, Integer> maxParallelismPerVertex) {
        final List<String> errors = new ArrayList<>();
        final Set<JobVertexID> missingJobVertexIds =
                new HashSet<>(maxParallelismPerVertex.keySet());
        for (JobVertexID jobVertexId : jobResourceRequirements.getJobVertices()) {
            final Optional<Integer> maybeMaxParallelism =
                    Optional.ofNullable(maxParallelismPerVertex.get(jobVertexId));
            if (maybeMaxParallelism.isPresent()) {
                final JobVertexResourceRequirements.Parallelism requestedParallelism =
                        jobResourceRequirements.findParallelism(jobVertexId).get();
                if (requestedParallelism.getLowerBound() > requestedParallelism.getUpperBound()) {
                    errors.add(
                            String.format(
                                    "The requested lower bound [%s] for job vertex [%s] is higher than the upper bound [%d].",
                                    requestedParallelism.getLowerBound(),
                                    jobVertexId,
                                    requestedParallelism.getUpperBound()));
                }
                if (maybeMaxParallelism.get() < requestedParallelism.getUpperBound()) {
                    errors.add(
                            String.format(
                                    "The newly requested parallelism %d for the job vertex %s exceeds its maximum parallelism %d.",
                                    requestedParallelism.getUpperBound(),
                                    jobVertexId,
                                    maybeMaxParallelism.get()));
                }
            } else {
                errors.add(
                        String.format(
                                "Job vertex [%s] was not found in the JobGraph.", jobVertexId));
            }
            missingJobVertexIds.remove(jobVertexId);
        }
        for (JobVertexID jobVertexId : missingJobVertexIds) {
            errors.add(
                    String.format(
                            "The request is incomplete, missing job vertex [%s] resource requirements.",
                            jobVertexId));
        }
        return errors;
    }

    public static JobResourceRequirements empty() {
        return JobResourceRequirements.EMPTY;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private final Map<JobVertexID, JobVertexResourceRequirements> vertexResources =
                new HashMap<>();

        public Builder setParallelismForJobVertex(
                JobVertexID jobVertexId, int lowerBound, int upperBound) {
            vertexResources.put(
                    jobVertexId,
                    new JobVertexResourceRequirements(
                            new JobVertexResourceRequirements.Parallelism(lowerBound, upperBound)));
            return this;
        }

        public JobResourceRequirements build() {
            return new JobResourceRequirements(
                    Collections.unmodifiableMap(new HashMap<>(vertexResources)));
        }
    }

    private final Map<JobVertexID, JobVertexResourceRequirements> vertexResources;

    public JobResourceRequirements(
            Map<JobVertexID, JobVertexResourceRequirements> vertexResources) {
        this.vertexResources = vertexResources;
    }

    public Optional<JobVertexResourceRequirements.Parallelism> findParallelism(
            JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(JobVertexResourceRequirements::getParallelism);
    }

    public Set<JobVertexID> getJobVertices() {
        return vertexResources.keySet();
    }

    public Map<JobVertexID, JobVertexResourceRequirements> getJobVertexParallelisms() {
        return vertexResources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JobResourceRequirements that = (JobResourceRequirements) o;
        return Objects.equals(vertexResources, that.vertexResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexResources);
    }

    @Override
    public String toString() {
        return "JobResourceRequirements{" + "vertexResources=" + vertexResources + '}';
    }
}
