package de.otto.synapse.configuration;

import org.junit.After;
import org.junit.Test;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties="spring.application.name=my service")
@EnableConfigurationProperties(SynapseProperties.class)
public class SynapsePropertiesTest {

    @Autowired
    private SynapseProperties synapseProperties;

    @Test
    public void shouldResolveSenderNamePlaceholder() {
        assertThat(synapseProperties.getSender().getName()).isEqualTo("my service");
    }

    //    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
//
//    @After
//    public void close() {
//        if (this.context != null) {
//            this.context.close();
//        }
//    }
//
//    @Test
//    public void shouldResolveSenderNamePlaceHolder() {
//        context.register(org.springframework.context.support.PropertySourcesPlaceholderConfigurer.class);
//        context.register(SynapseAutoConfiguration.class);
//        TestPropertyValues.of(
//                "spring.application.name=my service"
//        ).applyTo(context);
//
//        context.refresh();
//
//        assertThat(context.getBean(SynapseProperties.class).getSender().getName()).isEqualTo("my service");
//    }
}