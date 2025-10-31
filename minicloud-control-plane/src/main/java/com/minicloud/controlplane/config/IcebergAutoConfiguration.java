package com.minicloud.controlplane.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(IcebergConfiguration.class)
public class IcebergAutoConfiguration {
    // This class enables the IcebergConfiguration properties
}