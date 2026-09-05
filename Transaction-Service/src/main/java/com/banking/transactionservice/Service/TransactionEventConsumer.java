package com.banking.transactionservice.Service;

import com.banking.transactionservice.Entity.TransactionEntity;
import com.banking.transactionservice.Entity.TransactionStatus;
import com.banking.transactionservice.Repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String,String> redisTemplate;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    private static final long OTP_EXPIRY_MINUTES=5;

    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";
    private final TransactionService transactionService;

    /**
     * Consumer verification required
     * Generate OTP and ask user to verify
     */

    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(@Payload Map<String,Object> payload){
        try{
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("verification required  - transaction : {} reason : {} " , transactionId , reason);

            TransactionEntity transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException(
                                        "Transaction not found " + transactionId
                    ));

            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction {} not Processing - skipping " , transactionId);
                return;
            }

            // Generate 6 digit OTP
            String otp  = String.format("%06d" , (int) (Math.random() * 900000) + 100000);

            // Store otp in redis - OTP expire in 5 minute
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey , otp , OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);


            // Update status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction : {} expire in {} min " , transactionId , OTP_EXPIRY_MINUTES);


            // NOTIFY user
            // public otpEvent to Kafka

            Map<String,Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId" , transactionId);
            otpEvent.put("accountNumber" , accountNumber);
            otpEvent.put("reason" , reason);
            otpEvent.put("otp",otp);
            otpEvent.put("amount" , payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC , transactionId , otpEvent);
        }
        catch(Exception e){
            log.error("Error handling verification required : {} " , e.getMessage());
        }

    }
    @KafkaListener(topics = "fraud.check.clean")
    public void consumerFraudCheckCleanResult(@Payload Map<String,Object> payload){
        try{
            String transactionId=   (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);
        }
        catch (Exception e){
            log.error("Error processing fraud check result  : {} " , e.getMessage());

        }
    }




















}
