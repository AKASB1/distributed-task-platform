package io.akasb.taskplatform.config;

import io.akasb.taskplatform.dispatch.JobDispatcher;
import io.akasb.taskplatform.dispatch.rabbitmq.RabbitMqDispatcher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ transport for job ids, active with {@code taskplatform.dispatcher.type=rabbitmq} (replacing the in-memory
 * dispatcher of {@link PlatformConfiguration}). Connection settings come from {@code taskplatform.rabbitmq.*}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "taskplatform.dispatcher", name = "type", havingValue = "rabbitmq")
public class RabbitMqConfiguration {

    @Bean(destroyMethod = "close")
    JobDispatcher rabbitMqDispatcher(TaskPlatformProperties props) {
        TaskPlatformProperties.Rabbitmq r = props.getRabbitmq();
        return new RabbitMqDispatcher(new RabbitMqDispatcher.Settings(r.getHost(), r.getPort(), r.getUsername(),
                r.getPassword(), r.getVirtualHost(), r.getQueuePrefix(), r.getPrefetch()));
    }
}
