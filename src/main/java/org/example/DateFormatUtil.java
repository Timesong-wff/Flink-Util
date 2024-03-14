package org.example;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author Felix
 * @date 2023/9/27
 * 日期转换工具类
 * SimpleDateFormat存在线程安全问题
 * 在封装日期工具类的时候，建议使用jdk1.8后提供的日期包下的类
 */
public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final DateTimeFormatter dtfYMD = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    //private static final SimpleDateFormat dtfYDM = new SimpleDateFormat("yyyyMMddHHmm");
    public static Long toTs(String dtStr, boolean isFull) {

        LocalDateTime localDateTime = null;
        if (!isFull) {
            dtStr = dtStr + " 00:00:00";
        }
        localDateTime = LocalDateTime.parse(dtStr, dtfFull);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
    }

    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    //转换成yyyy-MM-dd HH:mm:ss
    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    //转换成yyyyMMddHHmm
    public static String toYmdHm(Long ts) {
        Date dateTs = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dateTs.toInstant(), ZoneId.systemDefault());
        return dtfYMD.format(localDateTime);
    }
}
