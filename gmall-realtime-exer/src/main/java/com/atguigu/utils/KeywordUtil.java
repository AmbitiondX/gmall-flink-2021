package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    // 使用IK分词器对字符串进行分词
    public static List<String> analyze(String text) throws IOException {

        ArrayList<String> resultList = new ArrayList<>();
        StringReader stringReader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,true);


        Lexeme next = ikSegmenter.next();

        while (next != null) {
            resultList.add(next.getLexemeText());
            next = ikSegmenter.next();
        }


        stringReader.close();
        return resultList;

    }

    public static void main(String[] args) throws IOException {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }


}
