package com.banking.accountservice.Service;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {
    private final AccountService accountService;

    /*
        --> consume transaction.completed event from kafka
        --> Credits receiver account


          NOTE :Transaction Service Kafka Producer hai. Woh transaction.completed topic par transaction event publish karta hai.
                Account Service Kafka Consumer hai.
                Woh transaction.completed topic ko listen karta hai aur event receive hone par receiver account ko credit karta hai.

     */

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){
        try{
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());
            log.info("Crediting amount : {} amount : {} " , receiverAccount ,amount);
            accountService.creditBalance(receiverAccount,amount);
        }
        catch (Exception e){
            log.error("Error Crediting account:{}" , e.getMessage());
        }
    }

    /*
        consume fraud.detected event from kafka
        Block the flagged account


     */

    /*
            Account Service "fraude.detection" topic ko consume karta hai.
            Fraud detect karna iska kaam nahi hai; ye sirf received accountNumber ko lekar account ko block karta hai.

     */
    @KafkaListener(topics = "fraude.detection")
    public void consumerFraudDetection(@Payload Map<String,Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected : {} " , accountNumber);
            accountService.blockAccount(accountNumber);
        }
        catch (Exception e){
            log.error("Error blocking account : {} " , e.getMessage());
        }
    }
}
