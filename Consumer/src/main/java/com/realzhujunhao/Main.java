package com.realzhujunhao;

import org.apache.pulsar.client.api.*;

public class Main {
	private static PulsarClient CLIENT;
	private static MessageListener LISTENER;
	private static Consumer CONSUMER;

	static {
		try {
			CLIENT = PulsarClient.builder()
					.serviceUrl("pulsar://realzhujunhao.com:6650")
					.build();

			LISTENER = (consumer, msg) -> {
				System.out.println("received: " + new String(msg.getData()));
				try {
					consumer.acknowledge(msg);
				} catch (PulsarClientException e) {
					System.out.println("ack failed");
					consumer.negativeAcknowledge(msg);
					throw new RuntimeException(e);
				}
			};

			CONSUMER = CLIENT.newConsumer()
					.topic("my-topic")
					.subscriptionName("java consumer")
					.messageListener(LISTENER)
					.subscribe();
		} catch (PulsarClientException e) {
			CLIENT = null;
			CONSUMER = null;
			System.out.println("Failed to initialize");
			System.exit(1);
		}
	}

	public static void main(String[] args) throws PulsarClientException {
		while (true) {

		}
	}
}