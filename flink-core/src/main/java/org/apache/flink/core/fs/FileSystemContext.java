package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;

import static org.apache.flink.util.Preconditions.checkState;

@Experimental
public class FileSystemContext {

    private static final ThreadLocal<FileSystemContext> CONTEXTS = new ThreadLocal<>();

    @Internal
    public static void initializeContextForThread(String name) {
        final FileSystemContext oldContext = CONTEXTS.get();

        checkState(
                null == oldContext,
                "Found an existing FileSystem context for this thread: %s "
                        + "This may indicate an accidental repeated initialization, or a leak of the"
                        + "(Inheritable)ThreadLocal through a ThreadPool.",
                oldContext);

        final FileSystemContext newContext = new FileSystemContext(name);
        CONTEXTS.set(newContext);
    }

    static FileSystem wrapWithContextWhenActivated(FileSystem fs) {
        final FileSystemContext ctx = CONTEXTS.get();
        return ctx != null && fs instanceof ContextAwareFileSystem ? ((ContextAwareFileSystem) fs).wrap(fs, ctx) : fs;
    }

    private final String name;

    private FileSystemContext(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
