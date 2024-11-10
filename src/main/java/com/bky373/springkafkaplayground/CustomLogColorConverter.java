package com.bky373.springkafkaplayground;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.pattern.color.ANSIConstants;
import ch.qos.logback.core.pattern.color.ForegroundCompositeConverterBase;

/**
 * logback-spring 로그 색상 지정 Custom Converter.
 */
public class CustomLogColorConverter extends ForegroundCompositeConverterBase<ILoggingEvent> {

    @Override
    protected String getForegroundColorCode(ILoggingEvent event) {
        return switch (event.getLevel().toString()) {
            case "TRACE" -> ANSIConstants.BLACK_FG;
            case "DEBUG" -> ANSIConstants.DEFAULT_FG;
            case "INFO" -> ANSIConstants.GREEN_FG;
            case "WARN" -> ANSIConstants.YELLOW_FG;
            case "ERROR" -> ANSIConstants.RED_FG;
            default -> ANSIConstants.DEFAULT_FG;
        };
    }

}
