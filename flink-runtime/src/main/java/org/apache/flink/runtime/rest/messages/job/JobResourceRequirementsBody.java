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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobResourceRequirements;
import org.apache.flink.runtime.jobmaster.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.TreeNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Body for change job requests. */
@JsonSerialize(using = JobResourceRequirementsBody.Serializer.class)
@JsonDeserialize(using = JobResourceRequirementsBody.Deserializer.class)
public class JobResourceRequirementsBody implements RequestBody, ResponseBody {

    public static class Serializer extends StdSerializer<JobResourceRequirementsBody> {

        public Serializer() {
            super(JobResourceRequirementsBody.class);
        }

        @Override
        public void serialize(
                JobResourceRequirementsBody body,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            final Map<JobVertexID, JobVertexResourceRequirements> perVertexRequirements =
                    body.getJobResourceRequirements()
                            .map(JobResourceRequirements::getJobVertexParallelisms)
                            .orElse(Collections.emptyMap());
            for (Map.Entry<JobVertexID, JobVertexResourceRequirements> entry :
                    perVertexRequirements.entrySet()) {
                jsonGenerator.writeObjectField(entry.getKey().toHexString(), entry.getValue());
            }
            jsonGenerator.writeEndObject();
        }
    }

    public static class Deserializer extends StdDeserializer<JobResourceRequirementsBody> {

        public Deserializer() {
            super(JobResourceRequirementsBody.class);
        }

        @Override
        public JobResourceRequirementsBody deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            final Map<JobVertexID, JobVertexResourceRequirements> perVertexRequirements =
                    new HashMap<>();
            final TreeNode rootNode = jsonParser.readValueAsTree();
            final Iterator<String> fieldIterator = rootNode.fieldNames();
            while (fieldIterator.hasNext()) {
                final String fieldName = fieldIterator.next();
                final JobVertexResourceRequirements vertexRequirements =
                        RestMapperUtils.getStrictObjectMapper()
                                .treeToValue(
                                        rootNode.get(fieldName),
                                        JobVertexResourceRequirements.class);
                perVertexRequirements.put(JobVertexID.fromHexString(fieldName), vertexRequirements);
            }
            return new JobResourceRequirementsBody(
                    new JobResourceRequirements(
                            Collections.unmodifiableMap(perVertexRequirements)));
        }
    }

    @Nullable private final JobResourceRequirements jobResourceRequirements;

    public JobResourceRequirementsBody(@Nullable JobResourceRequirements jobResourceRequirements) {
        this.jobResourceRequirements = jobResourceRequirements;
    }

    public Optional<JobResourceRequirements> getJobResourceRequirements() {
        return Optional.ofNullable(jobResourceRequirements);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JobResourceRequirementsBody that = (JobResourceRequirementsBody) o;
        return Objects.equals(jobResourceRequirements, that.jobResourceRequirements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobResourceRequirements);
    }
}
