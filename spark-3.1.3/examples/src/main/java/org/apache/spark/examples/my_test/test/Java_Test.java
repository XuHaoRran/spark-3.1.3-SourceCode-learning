package org.apache.spark.examples.my_test.test;

import java.util.*;

public class Java_Test {

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        // 注意 hasNext 和 hasNextLine 的区别
        while (in.hasNextLine()) { // 注意 while 处理多个 case
            String str1 = in.nextLine();
            String str2 = in.nextLine();
            String[] strs1 = str1.split(" ");
            String[] strs2 = str2.split(" ");
            int[] nums = new int[strs1.length];
            for(int i = 0; i < nums.length; i++){
                nums[i] = Integer.parseInt(strs1[i]);
            }
            int[] candy = new int[strs2.length];
            for(int i = 0; i < candy.length; i++){
                candy[i] = Integer.parseInt(strs2[i]);
            }

            Arrays.sort(nums);
            Arrays.sort(candy);
            int idx = 0;
            int maxIdx = candy.length;
            int res = 0;
            for(int i = 0; i < nums.length; i++){
                if(idx ==  maxIdx) return;

                while(idx < maxIdx && nums[i] > candy[idx]){
                    idx++;
                }
                if(nums[i] < candy[idx]) {
                    res++;
                    idx++;
                }
            }
            System.out.println(res);
        }
    }

}
