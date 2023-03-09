package org.apache.flink.docs.rest;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.annotation.docs.FlinkJsonSchema;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import java.util.Optional;

/** Helper methods for generation API documentation. */
public class ApiGeneratorUtils {

    private ApiGeneratorUtils() {}

    /**
     * Checks whether the given endpoint should be documented.
     *
     * @param spec endpoint to check
     * @return true if the endpoint should be documented
     */
    public static boolean shouldBeDocumented(
            MessageHeaders<
                            ? extends RequestBody,
                            ? extends ResponseBody,
                            ? extends MessageParameters>
                    spec) {
        return spec.getClass().getAnnotation(Documentation.ExcludeFromDocumentation.class) == null;
    }

    /**
     * Find whether the class contains dynamic fields that need to be documented.
     *
     * @param clazz class to check
     * @return optional that is non-empty if the class is annotated with {@link
     *     FlinkJsonSchema.AdditionalFields}
     */
    public static Optional<Class<?>> findAdditionalFieldType(Class<?> clazz) {
        final FlinkJsonSchema.AdditionalFields annotation =
                clazz.getAnnotation(FlinkJsonSchema.AdditionalFields.class);
        return Optional.ofNullable(annotation).map(FlinkJsonSchema.AdditionalFields::type);
    }
}
