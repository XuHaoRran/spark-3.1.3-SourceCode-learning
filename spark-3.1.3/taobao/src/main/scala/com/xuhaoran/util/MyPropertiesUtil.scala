package com.xuhaoran.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author xuhaoran 
 * @date 2023-04-19 19:17
 */
object MyPropertiesUtil {
    def main(args: Array[String]): Unit = {
        val properties: Properties = MyPropertiesUtil.load("config.properties")
        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName: String): Properties = {
        val prop = new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
            getResourceAsStream(propertieName), "UTF-8"))
        prop
    }


}
