package com.example.gcphellopubsub.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.pubsub.v1.PubsubMessage;

@Component
public class DemoConsumer extends PubSubConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(DemoConsumer.class);
	
	@Autowired
    private PubSubTemplate pubSubTemplate;
	
	@Value("${pubsub.subscription}")
    private String subscription;

	@Override
	public String subscription() {
		return this.subscription;
	}

	@Override
	protected void consume(BasicAcknowledgeablePubsubMessage basicAcknowledgeablePubsubMessage) {

		PubsubMessage message = basicAcknowledgeablePubsubMessage.getPubsubMessage();

        try {
            System.out.println(message.getData().toStringUtf8());
            System.out.println(message.getAttributesMap());
            String objectName = message.getAttributesMap().get("objectId");
            String bucketName = message.getAttributesMap().get("bucketId");
            String eventType = message.getAttributesMap().get("eventType");

            LOG.info("Event Type:::::" + eventType);
            LOG.info("File Name::::::" + objectName);
            LOG.info("Bucket Name::::" + bucketName);


        }catch(Exception ex) {
            LOG.error("Error Occured while receiving pubsub message:::::", ex);
        }
        basicAcknowledgeablePubsubMessage.ack();
	}

	@EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        LOG.info("==========> Subscribing {} to {} ", this.getClass().getSimpleName(), this.subscription());
        pubSubTemplate.subscribe(this.subscription(), this.consumer());
    }
}
