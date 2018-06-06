package de.otto.synapse.example.edison;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"de.otto"})
public class Server {
    public static void main(String[] args) {
        SpringApplication.run(Server.class, args);
    }
}
