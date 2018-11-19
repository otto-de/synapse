package de.otto.synapse.example.producer;

import de.otto.synapse.configuration.SynapseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(scanBasePackages = {"de.otto.synapse"})
@EnableConfigurationProperties(SynapseProperties.class)
public class Server {
    public static void main(String[] args) {
        SpringApplication.run(Server.class, args);
    }
}
