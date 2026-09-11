package org.example.trace.interceptor;

import org.example.common.trace.TraceContext;
import org.example.trace.TraceIdGenerator;
import org.example.trace.ZipkinReporter;

public class ServerTraceInterceptor {
    public static void beforeHandle(String traceId, String parentSpanId) {
        String spanId = TraceIdGenerator.generateSpanId();

        TraceContext.clear();
        TraceContext.setTraceId(traceId == null || traceId.isEmpty()
                ? TraceIdGenerator.generateTraceId() : traceId);
        if (parentSpanId != null && !parentSpanId.isEmpty()) {
            TraceContext.setParentSpanId(parentSpanId);
        }
        TraceContext.setSpanId(spanId);

        long startTimeStamp = System.currentTimeMillis();
        TraceContext.setStartTimeStamp(String.valueOf(startTimeStamp));
    }

    public static void afterHandle(String serviceName) {
        long endTimeStamp = System.currentTimeMillis();
        String start = TraceContext.getStartTimeStamp();
        if (start == null) {
            TraceContext.clear();
            return;
        }
        long startTimeStamp = Long.parseLong(start);
        long duration = endTimeStamp - startTimeStamp;

        try {
            ZipkinReporter.reportSpan(
                    TraceContext.getTraceId(),
                    TraceContext.getSpanId(),
                    TraceContext.getParentSpanId(),
                    "server-" + serviceName,
                    startTimeStamp,
                    duration,
                    serviceName,
                    "server"
            );
        } finally {
            TraceContext.clear();
        }
    }
}
