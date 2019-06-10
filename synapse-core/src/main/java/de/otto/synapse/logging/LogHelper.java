package de.otto.synapse.logging;

import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class LogHelper {

    public static void trace(final Logger logger, final Map<String, Object> contexts,
                             final String message, final Object[] msgParams) {
        log(logger, LogLevel.TRACE, contexts, message, msgParams, null);
    }

    public static void info(final Logger logger, final Map<String, Object> contexts,
                            final String message, final Object[] msgParams) {
        log(logger, LogLevel.INFO, contexts, message, msgParams, null);
    }

    public static void warn(final Logger logger, final Map<String, Object> contexts,
                            final String message, final Object[] msgParams) {
        log(logger, LogLevel.INFO, contexts, message, msgParams, null);
    }

    public static void error(final Logger logger, final Map<String, Object> contexts,
                             final String message, final Exception e) {
        log(logger, LogLevel.ERROR, contexts, message, null, e);
    }

    private static void log(final Logger logger, final LogLevel loglevel, final Map<String, Object> contexts, final String message, final Object[] msgParams, final Exception e) {
        putMDC(contexts);
        switch (loglevel) {
            case TRACE:
                if (logger.isTraceEnabled()) {
                    logger.trace(message != null ? message : "", msgParams);
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(message != null ? message : "", msgParams);
                }
                break;
            case INFO:
                logger.info(message != null ? message : "", msgParams);
                break;
            case WARN:
                logger.warn(message != null ? message : "", msgParams);
                break;
            case ERROR:
                logger.error(message != null ? message : "", msgParams, e);
                break;
            default:
                //Nicht möglich, wenn kein neuer Loglevel hinzugfügt wird.
                break;
        }
        removeMDC(contexts);
    }

    private static void putMDC(final String mdcKey, final Object value) {
        if (value != null) {
            MDC.put(mdcKey, String.valueOf(value));
        }
    }

    private static void removeMDC(final String mdcKey) {
        MDC.remove(mdcKey);
    }

    private static void putMDC(final Map<String, Object> contexts) {
        if (contexts != null) {
            for (final Map.Entry<String, Object> context : contexts.entrySet()) {
                putMDC(context.getKey(), context.getValue());
            }
        }
    }

    private static void removeMDC(final Map<String, Object> contexts) {
        if (contexts != null) {
            for (final String contextKey : contexts.keySet()) {
                removeMDC(contextKey);
            }
        }
    }

    public static double calculateMessagesPerSecond(AtomicLong lastMessageLogTime, long messagesSent) {
        long logTime = System.currentTimeMillis();
        long previousLogTime = lastMessageLogTime.getAndSet(logTime);
        long durationInMillis = logTime - previousLogTime;
        return 1d * messagesSent / (durationInMillis / 1000d);
    }

    private enum LogLevel {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR
    }
}