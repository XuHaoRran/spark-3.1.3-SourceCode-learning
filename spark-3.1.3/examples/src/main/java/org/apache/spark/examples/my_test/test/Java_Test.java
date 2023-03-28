package org.apache.spark.examples.my_test.test;

import org.codehaus.janino.Java;

import org.datanucleus.store.rdbms.identifier.IdentifierFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;
    TreeNode(int x) { val = x; }
}
public class Java_Test {

    // create table sql with table name "HAHA"
    // implement a function to create table sql using innodb engine
    // create table HAHA (id int, name varchar(20), age int, primary key(id))
    // create table HAHA (id int, name varchar(20), age int, primary key(id)) engine=innodb
    public static String createTable(String tableName, List<String> columns, List<String> types, String primaryKey) {
        String sql = "create table " + tableName + " (";
        for (int i = 0; i < columns.size(); i++) {
            sql += columns.get(i) + " " + types.get(i);
            if (i != columns.size() - 1) {
                sql += ", ";
            }
        }
        sql += ", primary key(" + primaryKey + ")) engine=innodb";
        return sql;
    }


}




