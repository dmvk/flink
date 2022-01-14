package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;

@Experimental
public interface ContextAwareFileSystem {

    FileSystem wrap(FileSystem fileSystem, FileSystemContext ctx);
}
