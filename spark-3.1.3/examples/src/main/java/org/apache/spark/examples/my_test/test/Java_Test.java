package org.apache.spark.examples.my_test.test;

import java.util.*;

public class Java_Test {

    public static void main(String[] args) {
        int x = 22;
        int[] nums = new int[10003];
        int t = 1;
        nums[1] = 1;
        for(int i = 2; i < nums.length; i++){
            nums[i] = nums[i - 1] + ++t;
        }
        int temp = x;
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        char[] red = new char[]{'r','e','d'};
        while(temp > 0){
            int mid = search(nums, temp);
            for (int i = 0; i < mid; i++) {
                sb.append(red[idx]);
            }
            idx = (idx + 1) % 3;
            temp -= nums[mid];
        }
        System.out.println(sb);

    }

    public static int search(int[] nums, int x){
        int l = 1;
        int r = nums.length - 1;
        while(l < r){
            int mid =  (l + r) / 2;
            if(nums[mid] < x){
                l = mid;
            }else if(nums[mid] > x){
                r = mid - 1;
            }else{
                return mid;
            }
        }
        return l;
    }

}
