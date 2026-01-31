package io.memris.logging;

import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.NOPLoggerFactory;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

public final class NoopSlf4jServiceProvider implements SLF4JServiceProvider {
    public static final String REQUESTED_API_VERSION = "2.0.0";

    private final NOPLoggerFactory loggerFactory = new NOPLoggerFactory();
    private final BasicMarkerFactory markerFactory = new BasicMarkerFactory();
    private final NOPMDCAdapter mdcAdapter = new NOPMDCAdapter();

    @Override
    public void initialize() {
        // No-op
    }

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return markerFactory;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return mdcAdapter;
    }

    @Override
    public String getRequestedApiVersion() {
        return REQUESTED_API_VERSION;
    }
}
