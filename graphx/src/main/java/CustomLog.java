import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.net.SyslogAppender;

/**
 * 自定义日志级别和
 */
public class CustomLog {
    private static CustomLog log = new CustomLog();

    private CustomLog() {
    }

    public static CustomLog getInstance() {
        return log;
    }

    /**
     * 继承Level自定义级别
     */
    private static class CustomLogLevel extends Level {
        private static final long serialVersionUID = 1L;

        protected CustomLogLevel(int level, String levelStr, int syslogEquivalent) {
            super(level, levelStr, syslogEquivalent);
        }
    }

    /**
     * 自定义级别名称，以及级别范围
     */
    private static final Level CustomerLevel = new CustomLogLevel(20050, "CUSTOMER", SyslogAppender.LOG_LOCAL0);

    /**
     * 生成日志对象
     *
     * @param filePath          日志输出路径
     * @param fileName          日志文件名
     * @param conversionPattern log的输出形式
     * @param flag              true:在已存在log文件后面追加 false:新log覆盖以前的log
     */
    public Logger createLogger(String filePath, String fileName, String conversionPattern, boolean flag) {
        // 生成新的Logger
        // 如果已经有了一个Logger实例则返回
        Logger logger = Logger.getLogger(fileName);
        // 清空Appender。特別是不想使用现存实例时一定要初期化
        logger.removeAllAppenders();
        // 设计定Logger级别。
        logger.setLevel(Level.DEBUG);
        // 设定是否继承父Logger。
        // 默认为true。继承root输出。
        // 设定false后将不输出root。
        logger.setAdditivity(false);
        // 生成新的Appender
        FileAppender appender = new RollingFileAppender();
        // log的输出形式
        PatternLayout layout = new PatternLayout();
        layout.setConversionPattern(conversionPattern + "%m%n");
        appender.setLayout(layout);
        // log输出路径
        appender.setFile(filePath + fileName + ".log");
        // log的文字码
        appender.setEncoding("UTF-8");
        // true:在已存在log文件后面追加 false:新log覆盖以前的log
        appender.setAppend(flag);
        // 适用当前配置
        appender.activateOptions();
        // 将新的Appender加到Logger中
        logger.addAppender(appender);

        return logger;
    }

    /**
     * 使用自定义日志打印logger中的log方法
     *
     * @param logger          日志对象
     * @param objLogInfo：日志内容
     */
    public void customLog(Logger logger, Object objLogInfo) {
        logger.log(CustomerLevel, objLogInfo);
    }

    /**
     * 关闭自定义log
     *
     * @param logger 日志对象
     */
    @SuppressWarnings("unchecked")
    public void closeCustomLog(Logger logger) {
        for (Enumeration<Appender> appenders = logger.getAllAppenders(); appenders.hasMoreElements(); ) {
            Appender appender = appenders.nextElement();
            appender.close();
        }
    }
}
