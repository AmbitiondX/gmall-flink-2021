package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * select count(*) from t1;
 * select id from t1;
 * select * from t1 where id=1001;  id是唯一键
 * select * from t1;
 */
public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws InstantiationException, IllegalAccessException, SQLException, InvocationTargetException {

        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            T t = clz.newInstance();
            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                Object object = resultSet.getObject(columnName);
                BeanUtils.setProperty(t, columnName, object);
            }
            list.add(t);
        }

        resultSet.close();
        preparedStatement.close();

        return list;

    }

    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> jsonObjects = queryList(connection, "select * from GMALL210426_REALTIME.DIM_BASE_CATEGORY1", JSONObject.class, false);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

    }

}