package org.apache.spark.examples.my_test.test;

import org.codehaus.janino.Java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Java_Test {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        // 注意 hasNext 和 hasNextLine 的区别
        while (sc.hasNextLine()) { // 注意 while 处理多个 case
            int n = Integer.parseInt(sc.nextLine());
            while(n-- > 0){
                Map<Long, List<Integer>> map = new HashMap<>();

                int k = Integer.parseInt(sc.nextLine());
                int res = 0;
                for (int i = 0; i < k; i++) {
                    String line = sc.nextLine();
                    String[] splits = line.split(" ");
                    int num = Integer.parseInt(splits[0]) * 2;
                    for (int j = 1; j < num; j+= 2) {
                        int a = Integer.parseInt(splits[j]);
                        int b = Integer.parseInt(splits[j  + 1]);
                        List<Integer> list = null;
                        list = map.getOrDefault((long)a * 100000000 + b, new ArrayList<>());
                        map.put((long)a * 100000000 + b, list);
                        if (list.size() == 0){
                          list.add(i);
                        } else if (list.get(list.size() - 1) + 1 == i){
                           list.add(i);
                        }else{
                            list = new ArrayList<>();
                        }
                        res = Math.max(res, list.size());
                    }
                }
                System.out.println(res);
            }
        }
    }





}
