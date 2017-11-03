package de.otto.edison.eventsourcing.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"de.otto.edison.eventsourcing"})
public class Server {
    public static void main(String[] args) {
        SpringApplication.run(Server.class, args);
    }
}
