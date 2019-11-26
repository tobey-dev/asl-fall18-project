package ch.ethz.asltest.Middleware.Log;

import org.apache.logging.log4j.*;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

/*
    The Log class is a wrapper over a Log4j Logger instance exposing a minimal interface.
 */

public class Log {
    private static Log ourInstance;

    public static void initialize(){
        ourInstance = new Log();
    }

    private static Logger logger;

    private Log() {
        logger = LogManager.getRootLogger();
        logger.info("[Log] Logging initialized");
    }

    public static void trace(String message){
        logger.trace(message);
    }

    public static void debug(String message){
        logger.debug(message);
    }

    public static void info(String message){
        logger.info(message);
    }

    public static void warn(String message){
        logger.warn(message);
    }

    public static void error(String message){
        logger.error(message);
    }

    public static void fatal(String message){
        logger.fatal(message);
    }

    public static void shutdown() {
        LogManager.shutdown();
    }
}
