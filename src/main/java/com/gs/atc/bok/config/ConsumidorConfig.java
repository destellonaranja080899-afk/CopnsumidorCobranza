package com.gs.atc.bok.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.gs.atc.bok.schemaRegistry.PagoCredito;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Configuration
public class ConsumidorConfig {

	private static final Logger log = LoggerFactory.getLogger(ConsumidorConfig.class);

	@Value("${bootstrap.server}")
	String bootstrapserver;

	@Value("${topic.name}")
	String topicname;

	@Value("${consumer.group.id}")
	String groupid;

	@Value("${auto.offset.reset}")
	String autoOffsetReset;

	@Value("${schema.registry.url}")
	String urlSchemaRegistry;

	@Value("${avro.specific.reader}")
	String specificReader;

	@Value("${path.keystore}")
	private String keystoreLocation;

	@Value("${path.truestore}")
	private String truestoreLocation;

	@Value("${keystore.password}")
	private String keystorePass;

	@Value("${truststore.password}")
	private String truestorePass;

	@Value("${protocol.securtity}")
	private String protocolSecurity;

	@Bean
	public Map<String, Object> consumerProperties() {
		Map<String, Object> props = new HashMap<>();

		try {
			File trustore = this.getResourceAsFile(truestoreLocation).getAbsoluteFile();
			File keystore = this.getResourceAsFile(keystoreLocation).getAbsoluteFile();
			
			// Configura las propiedades del sistema para el truststore 
            System.setProperty("javax.net.ssl.trustStore", trustore.getAbsolutePath()); 
            System.setProperty("javax.net.ssl.trustStorePassword", truestorePass);

			log.info(bootstrapserver);
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
			// props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
			props.put("schema.registry.url", urlSchemaRegistry);
			props.put("specific.avro.reader", specificReader);
//			props.put("auto.offset.reset", "latest");
			props.put("auto.offset.reset", "earliest");//desde el principio

			props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocolSecurity);
			props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustore.getAbsolutePath());// <--------trustore
			props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truestorePass);
			props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore.getAbsolutePath());// <--------keystore
			props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePass);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return props;
	}

	@Bean
	public ConsumerFactory<String, PagoCredito> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerProperties());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, PagoCredito> listenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, PagoCredito> listenerContainerFactoryIn = new ConcurrentKafkaListenerContainerFactory<>();
		listenerContainerFactoryIn.setConsumerFactory(consumerFactory());
		listenerContainerFactoryIn.setBatchListener(true);
		// listenerContainerFactoryIn.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
		return listenerContainerFactoryIn;
	}
	
	private File getResourceAsFile(String resourcePath) throws IOException {
        InputStream resourceStream = getClass().getClassLoader().getResourceAsStream(resourcePath);
        if (resourceStream == null) {
            throw new IOException("Resource not found: " + resourcePath);
        }
        File tempFile = File.createTempFile("tempfile", ".tmp");
        tempFile.deleteOnExit();
        Files.copy(resourceStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return tempFile;
    }

}
