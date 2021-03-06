package com.atguigu.app.udf;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String str) {

        try {

            List<String> stringList = KeywordUtil.analyze(str);
            for (String word : stringList) {
                collect(Row.of(word));
            }


        } catch (IOException e) {
            e.printStackTrace();
            collect(Row.of(str));
        }

    }

}
