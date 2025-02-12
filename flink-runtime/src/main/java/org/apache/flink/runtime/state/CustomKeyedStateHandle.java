package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface CustomKeyedStateHandle extends KeyedStateHandle {

    interface Serializer {

        CustomKeyedStateHandle deserialize(
                DataInputStream dis, ClassLoader userCodeClassLoader, @Nullable Path exclusiveDirPath)
                throws IOException;

        void serialize(CustomKeyedStateHandle handle, DataOutputStream dos) throws IOException;
    }

    Serializer getSerializer();
}
