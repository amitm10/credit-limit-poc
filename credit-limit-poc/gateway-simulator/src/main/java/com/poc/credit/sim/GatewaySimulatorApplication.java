package com.poc.credit.sim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Modes (--sim.mode=...):
 * <ul>
 *   <li>flow: continuous demo traffic with full order lifecycles, until stopped</li>
 *   <li>concurrency: the 500-way race against one account; exit code 0 = PASS</li>
 *   <li>load: fixed-rate load test with a latency report</li>
 * </ul>
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class GatewaySimulatorApplication {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context = SpringApplication.run(GatewaySimulatorApplication.class, args);
        int code = context.getBean(Simulator.class).run();
        System.exit(SpringApplication.exit(context, () -> code));
    }
}
