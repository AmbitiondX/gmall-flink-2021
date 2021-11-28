package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


public class KeywordUtil {
    public static List<String> analyze(String text) {
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, true);
    }
}
