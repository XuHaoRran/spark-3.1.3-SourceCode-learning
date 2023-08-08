package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

import java.util.ArrayList;

public class JSONUtils {
    public static boolean isJSONValidate(String log) {
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }

    }


}
