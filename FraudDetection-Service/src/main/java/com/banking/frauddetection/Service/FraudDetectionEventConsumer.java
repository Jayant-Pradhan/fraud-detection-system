package com.banking.frauddetection.Service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j

public class FraudDetectionEventConsumer {


    private final FraudDetectionService fraudDetectionService;

    public FraudDetectionEventConsumer(FraudDetectionService fraudDetectionService) {
        this.fraudDetectionService = fraudDetectionService;
    }

    /**
     * Listen to transaction.initiated topic from Kafka
     * every transaction goes through fraud check before completing .
     * @param payload
     */

    @KafkaListener(
            topics = "transaction.initiated",
            groupId = "fraud-detection-group"
    )
    public void consumeTransactionInitiated(@Payload Map<String,Object> payload){
        log.info("Received transaction for fraud check: {} " , payload.get("transactionId"));
        try{
            fraudDetectionService.fraudCheck(payload);
        }
        catch(Exception e){
            log.error("Fraud check failed : {} ", e.getMessage());
        }

    }
}
