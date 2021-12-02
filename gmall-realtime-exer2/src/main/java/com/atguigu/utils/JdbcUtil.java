package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    private static PreparedStatement preparedStatement;

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> t, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        ArrayList<T> list = new ArrayList<>();

        preparedStatement = connection.prepareStatement(sql);

        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();


        while (resultSet.next()) {

            T instance = t.newInstance();

            for (int i = 1; i < columnCount + 1; i++) {

                String columnName = metaData.getColumnName(i);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                Object value = resultSet.getObject(columnName);

                BeanUtils.setProperty(instance,columnName,value);

            }

            list.add(instance);

        }

        resultSet.close();
        preparedStatement.close();

        return list;

    }
}
