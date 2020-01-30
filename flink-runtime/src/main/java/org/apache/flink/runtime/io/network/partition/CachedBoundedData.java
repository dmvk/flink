package org.apache.flink.runtime.io.network.partition;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nullable;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedBoundedData implements BoundedData {

	private static final Logger LOG = LoggerFactory.getLogger(CachedBoundedData.class);

	private static final long CACHE_THRESHOLD = 4096;

	private static class CacheReader implements BoundedData.Reader {

		private final BlockingQueue<Buffer> queue;

		CacheReader(BlockingQueue<Buffer> queue) {
			this.queue = queue;
		}

		@Nullable
		@Override
		public Buffer nextBuffer() {
			return queue.poll();
		}

		@Override
		public void close() throws IOException {
			// Noop.
		}
	}

	private final BlockingQueue<Buffer> cache = new LinkedBlockingQueue<>();

	private final Path filePath;
	private final BoundedData.Factory delegateSupplier;

	private volatile boolean delegateInitialized = false;

	private BoundedData delegate;

	private long cacheSize = 0;

	CachedBoundedData(Path filePath, BoundedData.Factory delegateSupplier) {
		this.filePath = filePath;
		this.delegateSupplier = delegateSupplier;
	}

	@Override
	public void writeBuffer(Buffer buffer) throws IOException {
		if (delegateInitialized) {
			delegate.writeBuffer(buffer);
		} else {
			cache.add(buffer.retainBuffer());
			cacheSize += buffer.getSize();
			if (cacheSize > CACHE_THRESHOLD) {
				delegate = delegateSupplier.create();
				delegateInitialized = true;
				Buffer cachedBuffer;
				while ((cachedBuffer = cache.poll()) != null) {
					try {
						delegate.writeBuffer(cachedBuffer);
					} finally {
						cachedBuffer.recycleBuffer();
					}
					cacheSize -= cachedBuffer.getSize();
				}
				Preconditions.checkState(cacheSize == 0);
			}
		}
	}

	@Override
	public void finishWrite() throws IOException {
		if (delegateInitialized) {
			delegate.finishWrite();
		}
	}

	@Override
	public Reader createReader(ResultSubpartitionView subpartitionView) throws IOException {
		synchronized (this) {
			if (delegateInitialized) {
				return delegate.createReader(subpartitionView);
			}
			return new CacheReader(cache);
		}
	}

	@Override
	public long getSize() {
		if (delegateInitialized) {
			return delegate.getSize();
		}
		return cacheSize;
	}

	@Override
	public void close() throws IOException {
		synchronized (this) {
			if (delegateInitialized) {
				delegate.close();
				LOG.info("Shuffle channel [{}] final state: ON_DISK", filePath.toString());
			}
			LOG.info("Shuffle channel [{}] final state: IN_MEMORY", filePath.toString());
		}
	}
}
