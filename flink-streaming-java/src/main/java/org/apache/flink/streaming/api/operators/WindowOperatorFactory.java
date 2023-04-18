package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.FunctionWithException;

import java.util.Optional;

public class WindowOperatorFactory<OUT> extends SimpleUdfStreamOperatorFactory<OUT>
        implements YieldingTimersOperatorFactory<OUT> {

    public static class WatermarkHoldingOutput<OUT> implements Output<OUT> {

        private final Output<OUT> delegate;
        private final MailboxExecutor taskMailboxExecutor;
        private final MailboxExecutor mainMailboxExecutor;

        private WatermarkHoldingOutput(
                Output<OUT> delegate,
                MailboxExecutor taskMailboxExecutor,
                MailboxExecutor mainMailboxExecutor) {
            this.delegate = delegate;
            this.taskMailboxExecutor = taskMailboxExecutor;
            this.mainMailboxExecutor = mainMailboxExecutor;
        }

        @Override
        public void collect(OUT record) {
            delegate.collect(record);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void emitWatermark(Watermark mark) {
            throw new UnsupportedOperationException("This should never be called.");
        }

        public void emitWatermarkInTheMailbox(
                Watermark mark,
                FunctionWithException<Watermark, Optional<Watermark>, Exception> advanceWatermark) {
            taskMailboxExecutor.execute(
                    () -> {
                        Optional<Watermark> maybeProgressedWatermark;
                        while ((maybeProgressedWatermark = advanceWatermark.apply(mark))
                                .isPresent()) {
                            while (mainMailboxExecutor.tryYield()) {
                                // No-op.
                            }
                            delegate.emitWatermark(maybeProgressedWatermark.get());
                            emitWatermarkInTheMailbox(mark, advanceWatermark);
                        }
                    },
                    "progressWatermark");
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            delegate.emitWatermarkStatus(watermarkStatus);
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            delegate.collect(outputTag, record);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            delegate.emitLatencyMarker(latencyMarker);
        }
    }

    public WindowOperatorFactory(WindowOperator<?, ?, ?, OUT, ?> operator) {
        super(operator);
    }

    @Override
    public Output<StreamRecord<OUT>> wrapOutput(
            Output<StreamRecord<OUT>> output,
            MailboxExecutor taskMailboxExecutor,
            MailboxExecutor mainMailboxExecutor) {
        return new WatermarkHoldingOutput<>(output, taskMailboxExecutor, mainMailboxExecutor);
    }
}
