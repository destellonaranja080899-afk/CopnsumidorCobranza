package com.gs.atc.bok.services;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.gs.atc.bok.schemaRegistry.PagoCredito;
@Service
public class ConsumerSrv {

    private static final Logger log = LoggerFactory.getLogger(ConsumerSrv.class);
    
    public static List<ConsumerRecord<String, PagoCredito>> eventosProcesar = new ArrayList<>();

    @KafkaListener(containerFactory = "listenerContainerFactory", topics = { "#{'${topic.name}'.split(',')}" })
    public void listene(List<ConsumerRecord<String, PagoCredito>> events) {
        
        try {
        	eventosProcesar.addAll(events);
        	
            log.info("numero de poll de eventos |{}|", events.size());
            
        } catch (Exception ex) {
            log.error("Incidencia al mapear la información del mensage | Message {} | cause {} |", ex.getMessage(), ex.getCause());
        }
        
    }

}
