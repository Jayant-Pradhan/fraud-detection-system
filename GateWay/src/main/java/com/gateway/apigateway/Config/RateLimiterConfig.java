package com.gateway.apigateway.Config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import reactor.core.publisher.Mono;

@Configuration
public class RateLimiterConfig {

//    @Bean
//    public KeySpaceResolver keyResolver() {
//
//        return exchange -> {
//            String host = exchange.getResource()
//                    .getRemoteAddress()
//                    .getAddress()
//                    .getHostAddress();
//
//            System.out.println("Host Address: " + host);
//
//            return null;
//
//        };
//    }


}
