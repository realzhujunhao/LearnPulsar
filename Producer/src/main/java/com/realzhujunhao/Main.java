package com.realzhujunhao;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class Main {
	private static PulsarClient CLIENT;
	private static Producer<byte[]> PRODUCER;

	static {
		try {
			CLIENT = PulsarClient.builder()
					.serviceUrl("pulsar://realzhujunhao.com:6650")
					.build();

			PRODUCER = CLIENT.newProducer()
					.topic("my-topic")
					.create();
		} catch (PulsarClientException e) {
			CLIENT = null;
			PRODUCER = null;
			System.out.println("Failed to initialize");
			System.exit(1);
		}
	}

	public static void main(String[] args) throws PulsarClientException {
		PRODUCER.send("THIS IS A MESSAGE".getBytes());
		PRODUCER.closeAsync()
				.thenRun(() -> System.out.println("Producer closed"));
		CLIENT.closeAsync()
				.thenRun(() -> System.out.println("Client closed"));
	}

}