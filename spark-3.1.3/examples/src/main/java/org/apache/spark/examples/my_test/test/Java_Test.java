package org.apache.spark.examples.my_test.test;

import org.codehaus.janino.Java;

import java.util.*;

public class Java_Test {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        int n = Integer.parseInt(scan.nextLine());
        for(int i = 0; i < n; i++) {
            int[] nums = new int[n];
            String str = scan.nextLine();
            System.out.println(str);
            for(int j = 0; j < nums.length; j++){
                nums[j] = str.charAt(j) - '0';
            }
            System.out.println(search(nums));
        }



    }

    public static int search(int[] nums){
        // 计算左边全聚满1要走多久
        int count = 0;
        int left = 0;
        for(int i = 0; i < nums.length; i++){
            if(nums[i] == 0){
                count++;
            }else{
                left += count;
            }
        }
        // 计算右边全聚满1要走多久
        int right = 0;
        count = 0;
        for(int i = nums.length - 1; i >= 0; i--){
            if(nums[i] == 0){
                count++;
            }else{
                right += count;
            }
        }
        return Math.max(left, right);
    }





}
