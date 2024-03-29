package org.example;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wang ff
 * @date 2024年3月29日
 * 分词器 , 用于统计搜索中关键字,进行详细划分
 */
public class KeywordUtil {
    //分词
    
    public static void main(String[] args)  {
        System.out.println(analyze("王傅飞 陈颖").size());
    }
    
    public static List<String> analyze(String text) {
        List<String> keywordList = new ArrayList<>();
        //字符串读取器
        StringReader stringReader = new StringReader(text);
        //ik分词器 , 将词切分
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        //利用一个接受ik中的词 , 并写入List中
        try {
            Lexeme lexeme = null;
            while ((lexeme = ikSegmenter.next()) != null) {
                keywordList.add(lexeme.getLexemeText());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }
}
