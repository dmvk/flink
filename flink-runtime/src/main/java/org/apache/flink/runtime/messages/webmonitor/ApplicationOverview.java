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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ApplicationOverview implements InfoMessage, ResponseBody {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_STATUS = "status";
    public static final String FIELD_NAME_EXCEPTIONS = "exceptions";

    @JsonProperty(FIELD_NAME_STATUS)
    private final String status;

    @JsonProperty(FIELD_NAME_EXCEPTIONS)
    private final List<String> exceptions;

    public ApplicationOverview(ApplicationStatus status, List<String> exceptions) {
        this(status.name(), exceptions);
    }

    @JsonCreator
    public ApplicationOverview(
            @JsonProperty(FIELD_NAME_STATUS) String status,
            @JsonProperty(FIELD_NAME_EXCEPTIONS) List<String> exceptions) {
        this.status = status;
        this.exceptions = exceptions;
    }

    public String getStatus() {
        return status;
    }

    public List<String> getExceptions() {
        return exceptions;
    }
}
