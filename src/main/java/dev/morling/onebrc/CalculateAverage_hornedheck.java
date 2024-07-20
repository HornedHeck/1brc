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

import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_hornedheck {
    
    private static final String FILE = "./measurements.txt";
    private static final VectorSpecies<Short> SPECIES = ShortVector.SPECIES_PREFERRED;
    private static final int LANE_SIZE = SPECIES.length();
    private static final VectorSpecies<Short> CHAR_CONVERSION_SPECIE = ShortVector.SPECIES_64;
    private static final ShortVector MULTIPLER = ShortVector.fromArray(CHAR_CONVERSION_SPECIE, new short[]{
        100, 10, 1, 0
    }, 0);
    private static short[] tempDigits = new short[CHAR_CONVERSION_SPECIE.length()];
    private static HashMap<String, ResultRow> map = new HashMap<>(10000);
    
    public static void main( String[] args ) throws IOException {
        Arrays.fill(tempDigits, (short) '0');
        
        Stream<String> lines = Files.lines(Paths.get(FILE))
                                    .collect(Collectors.toUnmodifiableList())
                                    .stream();
        
        long start = System.currentTimeMillis();
        
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
            MeasurementAggregator::new,
            ( aggregator, measurement ) -> {
                aggregator.values[aggregator.used] = measurement.temp;
                aggregator.count++;
                if ( aggregator.used == LANE_SIZE - 1 ) {
                    aggregator.processVector();
                } else {
                    aggregator.used++;
                }
            },
            ( aggregator1, aggregator2 ) -> {
                aggregator1.sum += aggregator2.sum;
                aggregator1.min = Math.min(aggregator1.min, aggregator2.min);
                aggregator1.max = Math.max(aggregator1.max, aggregator2.max);
                aggregator1.count += aggregator2.count;
                int spareSpace = LANE_SIZE - aggregator1.used - aggregator2.used;
                if ( spareSpace < 0 ) {
                    System.arraycopy(
                        aggregator2.values,
                        0,
                        aggregator1.values,
                        aggregator1.used,
                        aggregator2.used + spareSpace
                    );
                    aggregator1.processVector();
                    System.arraycopy(
                        aggregator2.values,
                        aggregator2.used + spareSpace,
                        aggregator1.values,
                        0,
                        -spareSpace
                    );
                    aggregator1.used = -spareSpace;
                } else {
                    System.arraycopy(
                        aggregator2.values,
                        0,
                        aggregator1.values,
                        aggregator1.used,
                        aggregator2.used
                    );
                    if ( spareSpace == 0 ) {
                        aggregator1.processVector();
                    }
                }
                
                
                return aggregator1;
            },
            aggregator -> {
                int sum = aggregator.sum;
                int min = aggregator.min;
                int max = aggregator.max;
                
                for ( int i = 0; i < aggregator.used; i++ ) {
                    sum += aggregator.values[i];
                    min = Math.min(min, aggregator.values[i]);
                    max = Math.max(max, aggregator.values[i]);
                }
                
                return new ResultRow(
                    min / 10.0,
                    sum / 10.0 / aggregator.count,
                    max / 10.0
                );
            }
        );
        
        var res = lines.map(str -> {
            int length = str.length();
            if ( str.charAt(length - 4) == ';' ) {
                return new Measurement(
                    str.substring(0, length - 4),
                    parseShortVectorized(str, 3)
                );
            } else if ( str.charAt(length - 5) == ';' ) {
                return new Measurement(
                    str.substring(0, length - 5),
                    parseShortVectorized(str, 4)
                );
            } else {
                return new Measurement(
                    str.substring(0, length - 6),
                    parseShortVectorized(str, 5)
                );
            }
        }).collect(
            Collectors.groupingBy(
                ( m ) -> m.name,
                () -> map,
                collector
            )
        );
        
        System.out.println(new TreeMap<>(res));
        System.out.println(System.currentTimeMillis() - start);
    }
    
    private static short parseShort( String temp ) {
        int length = temp.length();
        int res = temp.charAt(length - 1) - '0'
            + ( temp.charAt(length - 3) - '0' ) * 10;
        char ch;
        if ( length < 4 ) {
            return (short) res;
        }
        ch = temp.charAt(length - 4);
        if ( ch == '-' ) {
            return (short) -res;
        }
        res += ch - '0';
        if ( length < 5 )
            return (short) res;
        return (short) -res;
    }
    
    private static short parseShortVectorized( String temp, int offset ) {
        int length = temp.length();
        tempDigits[2] = (short) temp.charAt(length - 1);
        //skip '.'
        tempDigits[1] = (short) temp.charAt(length - 3);
        short sign = 1;
        switch (offset) {
            case 4:
                char firstChar = temp.charAt(length - 4);
                if ( firstChar == '-' ) {
                    sign = -1;
                    tempDigits[0] = '0';
                } else {
                    tempDigits[0] = (short) firstChar;
                }
                break;
            case 5:
                sign = -1;
                tempDigits[0] = (short) temp.charAt(length - 4);
                break;
        }
        short tempModulus = ShortVector.fromArray(CHAR_CONVERSION_SPECIE, tempDigits, 0)
            .sub((short) '0')
            .mul(MULTIPLER)
            .reduceLanes(VectorOperators.ADD);
        return (short) ( tempModulus * sign );
    }
    
    private record Measurement(String name, short temp) {}
    
    private static class MeasurementAggregator {
        private int sum = 0;
        private int min = 999;
        private int max = -999;
        
        private short[] values = new short[LANE_SIZE];
        private int used;
        private int count;
        
        private void processVector() {
            ShortVector vect = ShortVector.fromArray(SPECIES, values, 0);
            used = 0;
            sum += vect.reduceLanes(VectorOperators.ADD);
            min = Math.min(min, vect.reduceLanes(VectorOperators.MIN));
            max = Math.max(max, vect.reduceLanes(VectorOperators.MAX));
        }
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
