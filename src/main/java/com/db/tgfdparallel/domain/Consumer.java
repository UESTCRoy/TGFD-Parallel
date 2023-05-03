package com.db.tgfdparallel.domain;

import com.db.tgfdparallel.config.ActiveMQProperties;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;

@Component
public class Consumer {
    private final ActiveMQProperties activeMQProperties;
    private Connection connection;
    private Session session;
    private MessageConsumer messageConsumer;

    @Autowired
    public Consumer(ActiveMQProperties activeMQProperties) {
        this.activeMQProperties = activeMQProperties;
    }

    public void connect(String queueName) {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    activeMQProperties.getUser(),
                    activeMQProperties.getPassword(),
                    activeMQProperties.getBrokerUrl()
            );

            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            messageConsumer = session.createConsumer(destination);
        } catch (JMSException e) {
            throw new RuntimeException("Unable to connect to ActiveMQ", e);
        }
    }

    public String receive() {
        try {
            Message message = messageConsumer.receive();
            if (message instanceof TextMessage) {
                return ((TextMessage) message).getText();
            }
        } catch (JMSException e) {
            throw new RuntimeException("Unable to receive message", e);
        }
        return null;
    }

    public void close() {
        try {
            if (messageConsumer != null) {
                messageConsumer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            throw new RuntimeException("Error closing ActiveMQ connection", e);
        }
    }

}

