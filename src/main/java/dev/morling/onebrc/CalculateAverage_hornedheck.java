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
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
    private static final short[] tempDigits = new short[CHAR_CONVERSION_SPECIE.length()];
    private static final MethodHandle mismatch;
    private static final Unsafe theUnsafe;
    private static final Map<CheapName, ResultRow> map = new HashMap<>(10000, 1.0f);
    private static final long VALUE_FIELD_OFFSET;
    private static final long SIZE_FIELD_OFFSET;
    private static final long BYTES_FIELD_OFFSET;
    private static char charBuff;
    private static short sign;
    private static int length;
    
    static {
        
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            theUnsafe = (Unsafe) f.get(null);
            
            VALUE_FIELD_OFFSET = fieldOffset(String.class, "value");
            SIZE_FIELD_OFFSET = fieldOffset(CheapName.class, "size");
            BYTES_FIELD_OFFSET = fieldOffset(CheapName.class, "bytes");
            
            Class<?> arraysSupport = Class.forName("jdk.internal.util.ArraysSupport");
            var lookup = MethodHandles.privateLookupIn(arraysSupport, MethodHandles.lookup());
            // int mismatch(byte[] a, int aFromIndex, byte[] b, int bFromIndex, int length)
            mismatch = lookup.findStatic(
                arraysSupport, "mismatch",
                MethodType.methodType(
                    int.class,
                    byte[].class,
                    int.class,
                    byte[].class,
                    int.class,
                    int.class
                )
            );
        } catch (Exception e) {
            throw new Error(e);
        }
    }
    
    private static <T> long fieldOffset( Class<T> cls, String name ) throws NoSuchFieldException {
        Field field = cls.getDeclaredField(name);
        return theUnsafe.objectFieldOffset(field);
    }
    
    public static void main( String[] args ) throws IOException, NoSuchFieldException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException {
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
                
                for ( int i = 0; i < aggregator.used; i++ ) {
                    aggregator.sum += aggregator.values[i];
                    aggregator.min = Math.min(aggregator.min, aggregator.values[i]);
                    aggregator.max = Math.max(aggregator.max, aggregator.values[i]);
                }
                
                return new ResultRow(
                    aggregator.min / 10.0,
                    aggregator.sum / 10.0 / aggregator.count,
                    aggregator.max / 10.0
                );
            }
        );
        
        var res = lines.map(str -> {
            int index = indexOfDel(str);
            return new Measurement(
                new CheapName(str, index),
                parseShortVectorized(str, index)
            );
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
    
    private static int indexOfDel( String src ) {
        length = src.length();
        if ( src.charAt(length - 4) == ';' ) {
            return length - 4;
        }
        if ( src.charAt(length - 5) == ';' ) {
            return length - 5;
        }
        return length - 6;
    }
    
    private static short parseShortVectorized( String temp, int index ) {
        tempDigits[2] = (short) temp.charAt(length - 1);
        //skip '.'
        tempDigits[1] = (short) temp.charAt(length - 3);
        sign = 1;
        switch (length - index - 1) {
            case 4:
                charBuff = temp.charAt(length - 4);
                if ( charBuff == '-' ) {
                    sign = -1;
                    tempDigits[0] = '0';
                } else {
                    tempDigits[0] = (short) charBuff;
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
    
    private record Measurement(CheapName name, short temp) {}
    
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
    
    private static class CheapName implements Comparable<CheapName> {
        
        private final byte[] bytes;
        private final int size;
        
        private int hashCode;
        
        public CheapName( String src, int size ) {
            this.bytes = (byte[]) theUnsafe.getObject(src, VALUE_FIELD_OFFSET);
            this.size = size;
            hashCode = 1;
            for ( int i = 0; i < size; ++i ) {
                hashCode = 31 * hashCode + bytes[i];
            }
        }
        
        @Override
        public int compareTo( CheapName other ) {
            int lim = Math.min(size, other.size);
            int k = -1;
            for ( int i = 0; i < lim; i++ ) {
                if ( bytes[i] != other.bytes[i] ) {
                    k = i;
                    break;
                }
            }
            return k < 0 ? size - other.size : bytes[k] - other.bytes[k];
        }
        
        @Override
        public int hashCode() {
            return hashCode;
        }
        
        @Override
        public boolean equals( Object obj ) {
//            Arrays.equals()
            return hashCode == obj.hashCode();
        }
    }
}
