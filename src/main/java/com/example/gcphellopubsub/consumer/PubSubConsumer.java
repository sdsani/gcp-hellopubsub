package com.example.gcphellopubsub.consumer;

import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Autowired;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;

public abstract class PubSubConsumer {
	
	@Autowired
    private PubSubTemplate pubSubTemplate;

    /* Name of the Subscription */
    public abstract String subscription();

    protected abstract void consume(BasicAcknowledgeablePubsubMessage message);

    public Consumer<BasicAcknowledgeablePubsubMessage> consumer() {
        return basicAcknowledgeablePubsubMessage -> consume(basicAcknowledgeablePubsubMessage);
    }

    public Subscriber consumeMessage() {
        return this.pubSubTemplate.subscribe(this.subscription(), this.consumer());
    }
}
