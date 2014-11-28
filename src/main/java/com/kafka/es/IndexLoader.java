package com.kafka.es;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.alibaba.fastjson.JSON;
import com.kafka.utils.PropertiesLoader;

public class IndexLoader {
	public static Log logger = LogFactory.getLog(IndexLoader.class);
	private final PropertiesLoader loader;

	public IndexLoader() {
		this.loader = new PropertiesLoader(
				new String[] { "kafka.properties" });
	}

	private Properties createConsumerProperties(String topic) {
		Properties props = new Properties();
		props.put("zk.connect", this.loader.getProperty("zookeeper.connect"));
		props.put("zk.sessiontimeout.ms", "30000");
		props.put("autooffset.reset", "largest");
		props.put("autocommit.enable", "true");
		props.put("socket.buffersize",
				this.loader.getProperty("kafka.client.buffer.size"));

		props.put("fetch.size",
				this.loader.getProperty("kafka.client.buffer.size"));
		props.put("groupid", this.loader.getProperty("kafka.groupid"));
		logger.info("Properties:" + props.toString());
		return props;
	}

	public KafkaStream<Message> createConsumer() throws IOException {
		String topic = this.loader.getProperty("topic");
		Properties props = createConsumerProperties(topic);
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumerConnector = Consumer
				.createJavaConsumerConnector(consumerConfig);

		Map topicMap = new HashMap();
		topicMap.put(topic, Integer.valueOf(1));
		Map topicMessageStreams = consumerConnector
				.createMessageStreams(topicMap);

		List streams = (List) topicMessageStreams.get(topic);
		KafkaStream stream = (KafkaStream) streams.get(0);
		return stream;
	}

	public static void main(String[] arg) throws IOException,
			InterruptedException, ExecutionException {
		IndexLoader f = new IndexLoader();
		SimpleDateFormat sdf = new SimpleDateFormat(
				f.loader.getProperty("index.rolling.fmt"));

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", f.loader.getProperty("cluster.name"))
				.build();

		TransportClient client = new TransportClient(settings);
		String hosts = f.loader.getProperty("cluster.hosts");
		String[] hostslist = hosts.split(",");
		for (String h : hostslist) {
			client.addTransportAddress(new InetSocketTransportAddress(h, 9300));
		}
		BulkRequestBuilder brb = client.prepareBulk();
		KafkaStream message = f.createConsumer();
		long i = 0L;
		ConsumerIterator<Message> it = message.iterator();
        while (it.hasNext()){
        	String json = new String(
					toByteArray(((Message) it.next().message()).payload()));
			Event e = (Event) JSON.parseObject(json, Event.class);
			IndexRequestBuilder irb = client.prepareIndex(
					f.loader.getProperty("index.name.prefix") + "_"
							+ sdf.format(new Date(e.getTimestamp())),
							it.next().topic()).setSource(JSON.toJSONString(e));
			brb.add(irb);
			i += 1L;
			if (i % 100L == 0L) {
				brb.execute().get();
				brb = client.prepareBulk();
			}
        }
//		for (MessageAndMetadata msgAndMetadata : message) {
//			String json = new String(
//					toByteArray(((Message) msgAndMetadata.message()).payload()));
//
//			Event e = (Event) JSON.parseObject(json, Event.class);
//			IndexRequestBuilder irb = client.prepareIndex(
//					f.loader.getProperty("index.name.prefix") + "_"
//							+ sdf.format(new Date(e.getTimestamp())),
//					msgAndMetadata.topic()).setSource(JSON.toJSONString(e));
//			brb.add(irb);
//			i += 1L;
//			if (i % 100L == 0L) {
//				brb.execute().get();
//				brb = client.prepareBulk();
//			}
//		}
	}

	public static byte[] toByteArray(ByteBuffer buffer) {
		byte[] ret = new byte[buffer.remaining()];
		buffer.get(ret, 0, ret.length);
		return ret;
	}
}
