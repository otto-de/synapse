package de.otto.synapse.edison.state;

import de.otto.edison.navigation.NavBar;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import static de.otto.edison.navigation.NavBar.emptyNavBar;

@Configuration
@Profile("test")
public class StateRepositoryControllerTestConfiguration {

    @Bean
    public NavBar rightNavBar() {
        return emptyNavBar();
    }

    @Bean
    public StateRepository<String> testStateRepository() {
        final StateRepository<String> test = new ConcurrentHashMapStateRepository<>("test");
        test.put("first", "one");
        test.put("second", "two");
        return test;
    }
}

