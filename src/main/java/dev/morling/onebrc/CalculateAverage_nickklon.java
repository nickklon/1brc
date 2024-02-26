/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_nickklon {

    private static final String FILE = "./measurements.txt";

    private static final byte NEWLINE = 0x0A;
    private static final long NEWLINE_LONG = 0x0AL;
    private static final byte SEMICOLON = 0x3B;
    private static final long SEMICOLON_LONG = 0x3BL;
    private static final long LSB_MASK = 0xFFL;

    static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        static MeasurementAggregator aggregate(MeasurementAggregator agg1, MeasurementAggregator agg2) {
            var res = new MeasurementAggregator();
            res.min = Math.min(agg1.min, agg2.min);
            res.max = Math.max(agg1.max, agg2.max);
            res.sum = agg1.sum + agg2.sum;
            res.count = agg1.count + agg2.count;
            return res;
        }

        void add(double m) {
            count++;
            sum += m;
            min = Math.min(m, min);
            max = Math.max(m, max);
        }
    }

    public static void main(String[] args) throws IOException {

        int concurrency = 12;
        Map<String, MeasurementAggregator> results = new HashMap<>(1000);

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

            var completionService = new ExecutorCompletionService<Map<String, MeasurementAggregator>>(executorService);

            for (int i = 0; i < concurrency; i++) {
                completionService.submit(new RandomAccessTask(i, concurrency));
            }

            int completed = 0;
            while (completed < concurrency) {
                var resultFuture = completionService.take();
                try {
                    var result = resultFuture.get();
                    aggregateMap(results, result);
                    completed++;
                }
                catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        var measurements = new TreeMap<>(
                results.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> new ResultRow(e.getValue().min,
                                        (Math.round(e.getValue().sum * 10.0) / 10.0) / e.getValue().count,
                                        e.getValue().max))));

        System.out.println(measurements);
    }

    private static void aggregateMap(Map<String, MeasurementAggregator> agg, Map<String, MeasurementAggregator> intermediate) {
        for (Map.Entry<String, MeasurementAggregator> entry : intermediate.entrySet()) {
            agg.merge(entry.getKey(), entry.getValue(), MeasurementAggregator::aggregate);
        }
    }

    private static class RandomAccessTask implements Callable<Map<String, MeasurementAggregator>> {

        final long splitNum;
        final long numSplits;

        RandomAccessTask(long splitNum, long numSplits) {
            this.numSplits = numSplits;
            this.splitNum = splitNum;
        }

        @Override
        public Map<String, MeasurementAggregator> call() throws Exception {

            try (RandomAccessFile file = new RandomAccessFile(FILE, "r")) {

                long bytesPerSplit = file.length() / numSplits;
                long firstByte = splitNum * bytesPerSplit;
                long lastByteExclusive = firstByte + bytesPerSplit;
                if (numSplits - splitNum == 1) {
                    lastByteExclusive = file.length();
                }

                /*
                The general algo is:
                1) we check if the previous split ends perfect with a newline
                2) We read from the first newLine up until the last newLine AT or AFTER lastByteEclusive
                 */

                var reader = new BufferingRandomAccessFileReader(firstByte, lastByteExclusive, file);

                Map<String, MeasurementAggregator> map = new HashMap<>(1000);

                var m = reader.readNext();
                while (m != null) {

                    var agg = map.get(m.station);
                    if (agg == null) {
                        agg = new MeasurementAggregator();
                        map.put(m.station, agg);
                    }
                    agg.add(m.value);

                    m = reader.readNext();
                }

                System.out.println(STR."Finished split \{splitNum}");
                return map;
            }
        }
    }

    static class BufferingRandomAccessFileReader {
        private final long firstByte;
        private final long lastByteExclusive;
        private long currentByte;
        private final RandomAccessFile file;

        private static final int bufSize = 8192;
        private static final int arrSize = 1024;

        private final byte[] bufArr = new byte[bufSize];
        private final ByteBuffer buf;
        private final byte[] arr = new byte[arrSize];


        BufferingRandomAccessFileReader(long firstByte, long lastByte, RandomAccessFile file) throws IOException {
            this.firstByte = firstByte;
            currentByte = firstByte;
            this.lastByteExclusive = lastByte;
            this.file = file;

            buf = ByteBuffer.allocate(bufSize);

            init();
        }

        public Measurement readNext() throws IOException, RuntimeException {

            if (currentByte >= lastByteExclusive) {
                return null;
            }

            int index = 0;
            int semicolon = Integer.MAX_VALUE;
            int newline = -1;
            int i;
            byte b;
            long l;

            while (true) {

                if (buf.remaining() >= 8) {

                    l = buf.getLong(); // it would seem earlier bytes end up in most-sig-bits of this long

                    // Copy long bytes to array (reading the LSBs as the later bytes)
                    for (i = 7; i >= 0; i--) {

                        if ((l & LSB_MASK) == NEWLINE_LONG)
                            newline = i; // Take earliest newline. Since newline is a breaking condition, ok
                        else if ((l & LSB_MASK) == SEMICOLON_LONG)
                            semicolon = Math.min(index + i, semicolon); // we may see semicolons after newline on the n+1st read, so need min

                        arr[i + index] = (byte)(l & LSB_MASK);
                        l >>= 8;
                    }


                    if (newline >= 0) {
                        index += newline;

                        // Set back the buffer position to after the newline
                        // e.g if newline is the final byte (ix 7), we dont actually set back
                        // but if newline is the first byte (ux 0), we have read 8 bytes but need to backtrack 7
                        buf.position(buf.position() - (7 - newline));
                        break;
                    }

                    index += 8;

                } else if (buf.hasRemaining()) {
                    // else read single
                    b = buf.get();
                    currentByte++;

                    if (b == NEWLINE) {
                        break;
                    } else if (b == SEMICOLON) {
                        semicolon = index;
                    }

                    arr[index++] = b;

                } else {
                    readToBuffer();
                    if (!buf.hasRemaining()) return null;
                }
            }

            String station = new String(arr, 0, semicolon);
            double value = Double.parseDouble(new String(arr, semicolon + 1, index - semicolon - 1));
            return new Measurement(station, value);
        }

        private void init() throws IOException {

            if (firstByte == 0)
                readToBuffer();
            else  {
                file.seek(firstByte - 1L);
                currentByte--;
                readToBuffer();

                // read up thru the first newline
                byte b = buf.get();
                currentByte++;
                while (b != NEWLINE) {
                    if (!buf.hasRemaining()) {
                        readToBuffer();
                    }
                    b = buf.get();
                    currentByte++;
                }
            }
        }

        private void readToBuffer() throws IOException {
            buf.clear();
            int read = file.read(bufArr);
            if (read > -1) buf.put(bufArr, 0, read);
            buf.flip();
        }

    }
}
