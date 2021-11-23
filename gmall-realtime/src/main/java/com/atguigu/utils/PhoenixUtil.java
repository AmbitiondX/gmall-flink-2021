package com.atguigu.utils;

import com.atguigu.bean.TM;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//ORM
public class PhoenixUtil {

    // 声明链接
    public static Connection connection = null;

    // 创建Phoenix连接
    public static Connection getConnection() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            return DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }

    // 查询Phoenix数据的方法
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {

        // 创建结果集合
        ArrayList<T> list = new ArrayList<>();
        PreparedStatement preparedStatement = null;

        try {

            if (connection == null) {
                connection = getConnection();
            }

            // 预编译SQL
            preparedStatement = connection.prepareStatement(sql);

            // 执行查询
            ResultSet resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 解析查询结果
            while (resultSet.next()) {

                // 构建泛型对象
                T t = clz.newInstance();

                for (int i = 0; i < columnCount; i++) {

                    // 取出列名
                    String columnName = metaData.getColumnName(i);

                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL).convert(columnName);
                    }

                    // 取出数据
                    Object value = resultSet.getObject(i);

                    // 将值设置给对象
                    BeanUtils.setProperty(t,columnName,value);
                }

                // 将当前对象添加至集合
                list.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        // 返回数据
        return list;
    }

    public static void main(String[] args) {
        System.out.println(queryList("select * from GMALL201109_REALTIME.DIM_BASE_TRADEMARK where id='1001'", TM.class,true));
    }

}
