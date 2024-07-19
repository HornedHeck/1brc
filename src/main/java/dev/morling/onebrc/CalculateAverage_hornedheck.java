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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_hornedheck {
    
    private static final String FILE = "./measurements.txt";
    
    public static void main( String[] args ) throws IOException {
        Stream<String> lines = Files.lines(Paths.get(FILE))
                                    .collect(Collectors.toUnmodifiableList())
                                    .stream();
        
        long start = System.currentTimeMillis();
        
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
            MeasurementAggregator::new,
            ( aggregator, measurement ) -> {
                aggregator.sum += measurement.temp;
                aggregator.count++;
                aggregator.min = Math.min(aggregator.min, measurement.temp);
                aggregator.max = Math.max(aggregator.max, measurement.temp);
            },
            ( aggregator1, aggregator2 ) -> {
                aggregator1.count += aggregator2.count;
                aggregator1.sum += aggregator2.sum;
                aggregator1.min = Math.min(aggregator1.min, aggregator2.min);
                aggregator1.max = Math.max(aggregator1.max, aggregator2.max);
                return aggregator1;
            },
            aggregator -> new ResultRow(
                aggregator.min / 10.0,
                aggregator.sum / 10.0 / aggregator.count,
                aggregator.max / 10.0
            )
        );
        
        var res = lines.map(str -> {
            String[] parts = str.split(";");
            return new Measurement(parts[0], parseInt(parts[1]));
        }).collect(
            Collectors.groupingBy(
                ( m ) -> m.name,
                collector
            )
        );
        
        System.out.println(new TreeMap<>(res));
        System.out.println(System.currentTimeMillis() - start);
    }
    
    private static int parseInt( String temp ) {
        int length = temp.length();
        int res = temp.charAt(length - 1) - '0'
            + ( temp.charAt(length - 3) - '0' ) * 10;
        char ch;
        if ( length < 4 ) {
            return res;
        }
        ch = temp.charAt(length - 4);
        if ( ch == '-' ) {
            return -res;
        }
        res += ch - '0';
        if ( length < 5 )
            return res;
        return -res;
    }
    
    private record Measurement(String name, int temp) {}
    
    private static class MeasurementAggregator {
        
        private int min = 999;
        private int max = -999;
        private int sum = 0;
        private long count = 0;
        
        public MeasurementAggregator() {}
    }
    
    private static record ResultRow(double min, double mean, double max) {
        
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }
        
        private double round( double value ) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
