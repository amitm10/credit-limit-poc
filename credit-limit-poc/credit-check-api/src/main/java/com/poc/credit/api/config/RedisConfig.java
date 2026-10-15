package com.poc.credit.api.config;

import com.poc.credit.common.redis.CreditScripts;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisConfig {

    @Bean
    CreditScripts creditScripts(StringRedisTemplate redis) {
        return new CreditScripts(redis);
    }

    /**
     * Redis WAIT confirms replication of the writes made on the connection that calls it.
     * Lettuce shares one multiplexed connection by default, where a blocking WAIT would stall
     * every request behind it. With a pool of dedicated connections, each check runs its
     * reserve script and its WAIT on its own connection.
     */
    @Bean
    static BeanPostProcessor dedicatedRedisConnections() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String beanName) {
                if (bean instanceof LettuceConnectionFactory factory) {
                    factory.setShareNativeConnection(false);
                }
                return bean;
            }
        };
    }
}
