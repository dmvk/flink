package org.apache.flink.runtime.io.network.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nullable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedBoundedData implements BoundedData {

	private static final Logger LOG = LoggerFactory.getLogger(CachedBoundedData.class);

	private static final long CACHE_THRESHOLD = 4096;

	/**
	 * Turn pooled buffer into unpooled on heap buffer, that can safely be cached.
	 *
	 * @param buffer Buffer with input data.
	 * @return New un-pooled on heap buffer.
	 */
	private static Buffer toUnpooledOnHeapBuffer(Buffer buffer) {
		final ByteBuffer readableBuffer = buffer.getNioBufferReadable();
		final byte[] bytes = new byte[readableBuffer.remaining()];
		readableBuffer.get(bytes);
		final MemorySegment memorySegment = MemorySegmentFactory.wrap(bytes);
		return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE, buffer.isBuffer(), buffer.getSize());
	}

	private static class CacheReader implements BoundedData.Reader {

		private final Iterator<Buffer> it;

		CacheReader(Iterator<Buffer> it) {
			this.it = it;
		}

		@Nullable
		@Override
		public Buffer nextBuffer() {
			if (it.hasNext()) {
				return it.next();
			}
			return null;
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
			if (cacheSize + buffer.getSize() > CACHE_THRESHOLD) {
				// Threshold reached, initialize delegate.
				delegate = delegateSupplier.create();
				delegateInitialized = true;
				// Flush cached buffers.
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
				// Write current buffer.
				delegate.writeBuffer(buffer);
			} else {
				cacheSize += buffer.getSize();
				final Buffer unpooledBuffer = toUnpooledOnHeapBuffer(buffer);
				cache.add(unpooledBuffer);
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
			return new CacheReader(cache.iterator());
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
			final String finalState;
			if (delegateInitialized) {
				delegate.close();
				finalState = "ON_DISK";
			} else {
				// Recycle cached buffers.
				Buffer cachedBuffer;
				while ((cachedBuffer = cache.poll()) != null) {
					cachedBuffer.recycleBuffer();
					cacheSize -= cachedBuffer.getSize();
				}
				Preconditions.checkState(cacheSize == 0);
				finalState = "IN_MEMORY";
			}
			LOG.info("Shuffle channel [{}] final state: {}.", filePath.toString(), finalState);
		}
	}
}
