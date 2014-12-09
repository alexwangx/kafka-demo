package com.kafka.demo;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlumeKafkaSink extends EventSink.Base {
	private static final Logger LOG = LoggerFactory.getLogger(FlumeKafkaSink.class);
	public static final String USAGE = "usage: kafka(\"zk.connect\", \"topic\")";
	private Producer<String, byte[]> producer;
	private String zkConnect;
	private String topic;

	public FlumeKafkaSink(String zkConnect, String topic) {
		this.zkConnect = zkConnect;
		this.topic = topic;
	}

	@Override
	public synchronized void open() throws IOException {
		Preconditions
				.checkState(
						this.producer == null,
						"Kafka sink is already initialized. Looks like sink close() hasn't proceeded properly.");

		Properties properties = new Properties();
		properties.setProperty("zk.connect", this.zkConnect);
		properties.setProperty("serializer.class", ByteEncoder.class.getName());
		ProducerConfig config = new ProducerConfig(properties);
		this.producer = new Producer<String, byte[]>(config);
		LOG.info("Kafka sink successfully opened");
	}

	@Override
	public void append(Event e) throws IOException {
		byte[] partition = e.get("kafka.partition.key");
		if (partition == null) {
			this.producer.send(new ProducerData<String, byte[]>(topic, e.getBody()));
		} else {
			this.producer.send(new ProducerData<String, byte[]>(topic, new String(
					partition, "UTF-8"), Lists.newArrayList(e.getBody())));
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (this.producer != null) {
			this.producer.close();
			this.producer = null;
			LOG.info("Kafka sink successfully closed");
		} else {
			LOG.warn("Double close of Kafka sink");
		}
	}

	public static SinkBuilder builder() {
		return new SinkBuilder() {
			// construct a new parameterized sink
			@Override
			public EventSink build(Context context, String... argv) {
				Preconditions.checkArgument(argv.length == 2,
						"usage: kafka(\"zk.connect\", \"topic\")");

				String zkConnect = argv[0];
				String topic = argv[1];

				Preconditions.checkArgument(!Strings.isNullOrEmpty(zkConnect),
						"zk.connect cannot be empty");
				Preconditions.checkArgument(!Strings.isNullOrEmpty(topic),
						"topic cannot be empty");

				return new FlumeKafkaSink(zkConnect, topic);
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this
	 * class as a plugin sink.
	 */
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
		List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("kafka", builder()));
		return builders;
	}

	public static class ByteEncoder implements Encoder<byte[]> {
		public Message toMessage(byte[] bytes) {
			return new Message(bytes);
		}
	}
}