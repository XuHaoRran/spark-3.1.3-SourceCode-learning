package org.apache.spark.examples.my_test.test;

public class BinarySearch {
    public static void main(String[] args) {
        int[] nums = new int[]{1,3,4,5,6,10,12};
        int left = 0;
        int right = nums.length;
        int x = 3;
        while(left < right){
            int mid = (left + right) / 2;
            if(nums[mid] < x){
                left = mid + 1;
            }else if(nums[mid] > x){
                right = mid;
            }else{
                left = mid;
                break;
            }
        }
        System.out.println(left);
    }
}
