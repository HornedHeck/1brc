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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_hornedheck {
    
    private static final String FILE = "./measurements.txt";
    
    public static void main( String[] args ) throws IOException {
        List<String> lines = Files.lines(Paths.get(FILE)).toList();
        
        long start = System.currentTimeMillis();
        
        Map<String, MeasurementAggregator> results = new HashMap<>();
        
        lines.forEach(line -> {
            String[] parts = line.split(";");
            if ( results.containsKey(parts[0]) ) {
                results.get(parts[0]).update(
                    parseInt(parts[1]));
            } else {
                results.put(parts[0], new MeasurementAggregator(
                    parseInt(parts[1])));
            }
        });
        
        System.out.println(new TreeMap<>(results));
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
    
    
    private static class MeasurementAggregator {
        
        private int min;
        private int max;
        private int sum;
        private long count;
        
        public MeasurementAggregator( int temp ) {
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }
        
        void update( int temp ) {
            sum += temp;
            count++;
            if ( min > temp ) {
                min = temp;
            } else if ( max < temp ) {
                max = temp;
            }
        }
    }
    
}
