package org.apache.spark.examples.my_test.test;

import org.datanucleus.store.rdbms.identifier.IdentifierFactory;

import java.util.HashMap;
import java.util.HashSet;

public class Java_Test2 {
    static int max = 0;
    static HashSet<Integer> set = new HashSet<>();
    public static void main(String[] args) {
        int n = 3;
        int m = 4;
        getMax(n, m);
        System.out.println(max);
    }
    public static void getMax(int n, int m){
        max = Math.max(max, 2 * n + 2 * m);
        set.add(n);
        set.add(m);
        if(n % 2 == 0 && !set.contains(n / 2)){
            getMax(n / 2, m * 2);
        }
        if (m % 2 == 0 && !set.contains(m / 2)){
            getMax(n * 2, m / 2);
        }
    }







}
