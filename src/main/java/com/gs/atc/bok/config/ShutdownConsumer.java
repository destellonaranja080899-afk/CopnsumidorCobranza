package com.gs.atc.bok.config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.gs.atc.bok.DAO.PagoCreditoDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.gs.atc.bok.schemaRegistry.PagoCredito;
import com.gs.atc.bok.services.ConsumerSrv;


@Component
public class ShutdownConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownConsumer.class);

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	@Autowired
	private PagoCreditoDAO pagoCreditoDAO;

	/**
	 * @var: constante para definir el limite de tiempo que consume los eventos el batch
	 * */
	private static final long LIMITE_TIEMPO = 180000;
	
	/**
	 * @var: Lista de eventos consumidos durante el tiempo establecido en LIMITE_TIEMPO y apaga el servicio
	 * */
	private List<ConsumerRecord<String, PagoCredito>> eventosConsumidos = ConsumerSrv.eventosProcesar;



	/**
	 * @method: metodo que detiene el consumo del batch y realiza el proceso de inserción en la bd
	 */
	@Scheduled(initialDelay = LIMITE_TIEMPO, fixedRate = Long.MAX_VALUE)
	public void shutdownConsumer() {
		kafkaListenerEndpointRegistry.stop();

		LOGGER.info("===> Total mensajes={}, ==> tiempo=[{}]", eventosConsumidos.size(),
				String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(LIMITE_TIEMPO),
						TimeUnit.MILLISECONDS.toSeconds(LIMITE_TIEMPO)
								- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(LIMITE_TIEMPO))));		
		insertsBD();
		clouseApplicationMicroservice();
		
	}

	/**
	 * @method: Metodo que detiene por completo el microservicio
	 */
	private void clouseApplicationMicroservice() {
		Runtime.getRuntime().halt(0);
	}

	private void insertsBD() {
		if (eventosConsumidos == null || eventosConsumidos.isEmpty()) {
			LOGGER.info("No hay mensajes para insertar en BD");
			return;
		}

		List<PagoCredito> pagos = new ArrayList<>();
		for (ConsumerRecord<String, PagoCredito> cr : eventosConsumidos) {
			pagos.add(cr.value());
			LOGGER.info("Preparando para insertar en BD: {}", cr.value().getOperacion().getClienteUnico());
		}

		try {
			pagoCreditoDAO.insertarBatch(pagos);
		} catch (Exception e) {
			LOGGER.error("Error insertando batch en BD", e);
		}
	}
}
