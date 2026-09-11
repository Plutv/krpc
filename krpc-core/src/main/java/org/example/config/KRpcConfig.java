package org.example.config;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@ToString
public class KRpcConfig {
    @Builder.Default
    private String name = "krpc";

    @Builder.Default
    private Integer port = 9999;

    @Builder.Default
    private String host = "localhost";

    @Builder.Default
    private String version = "1.0.0";

    @Builder.Default
    private String registry = "zookeeper";

    @Builder.Default
    private String serializer = "json";

    @Builder.Default
    private String loadBalance = "consistencyHash";

    @Builder.Default
    private Boolean tracingEnabled = true;

    @Builder.Default
    private Integer businessThreads = Math.max(4,
            Math.min(16, Runtime.getRuntime().availableProcessors() * 2));

    @Builder.Default
    private Integer businessQueueCapacity = 1024;

    @Builder.Default
    private String registryAddress = "127.0.0.1:2181";

    @Builder.Default
    private Integer registrySessionTimeoutMillis = 40000;

    @Builder.Default
    private Integer nodeFailureThreshold = 3;

    @Builder.Default
    private Integer nodeProbeIntervalSeconds = 10;

    @Builder.Default
    private Integer nodeProbeTimeoutMillis = 800;
}
