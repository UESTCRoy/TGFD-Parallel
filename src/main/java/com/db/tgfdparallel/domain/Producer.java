package com.db.tgfdparallel.domain;

import com.db.tgfdparallel.config.ActiveMQProperties;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;

@Component
public class Producer {
    private final ActiveMQProperties activeMQProperties;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;

    @Autowired
    public Producer(ActiveMQProperties activeMQProperties) {
        this.activeMQProperties = activeMQProperties;
    }

    public void connect() {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    activeMQProperties.getUser(),
                    activeMQProperties.getPassword(),
                    activeMQProperties.getBrokerUrl()
            );

            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new RuntimeException("Unable to connect to ActiveMQ", e);
        }
    }

    public void send(String dstQueue, String messageText) {
        try {
            Destination destination = session.createQueue(dstQueue);
            messageProducer = session.createProducer(destination);
            TextMessage message = session.createTextMessage(messageText);
            messageProducer.send(message);
        } catch (JMSException e) {
            throw new RuntimeException("Unable to send message to " + dstQueue, e);
        }
    }

    public void close() {
        try {
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            throw new RuntimeException("Unable to close ActiveMQ resources", e);
        }
    }
}
