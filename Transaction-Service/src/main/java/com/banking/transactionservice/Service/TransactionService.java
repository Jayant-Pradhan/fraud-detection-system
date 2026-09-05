package com.banking.transactionservice.Service;

import com.banking.transactionservice.Client.AccountServiceClient;
import com.banking.transactionservice.DTO.TransactionResponse;
import com.banking.transactionservice.DTO.TransferRequest;
import com.banking.transactionservice.Entity.TransactionEntity;
import com.banking.transactionservice.Entity.TransactionStatus;
import com.banking.transactionservice.Entity.TransactionType;
import com.banking.transactionservice.Event.TransactionCompletedEvent;
import com.banking.transactionservice.Event.TransactionInitiatedEvent;
import com.banking.transactionservice.Repository.TransactionRepository;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.cloud.client.loadbalancer.BlockingLoadBalancerInterceptor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor

public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;



    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    /**
     *  SAGA step - 1 : Initiate transfer
     *  Deducts from sender via feign
     *  saves transaction as PROCESSING
     *  public event to Kafka for fraud check
     *  Return
     *  @param request
     *  @return
     */

    public TransactionResponse transfer(@Valid TransferRequest request) {

        log.info("SAGA START - Transfer : {} -> {} amount : {} " ,
                request.getSenderAccountNumber() ,
                request.getReceiverAccountNumber(),
                request.getAmount());

        // SAGA Step : 1 Deduct From sender
        // SAGA Step 1: Use Feign Client to call Account-Service
        // and deduct the transfer amount from the sender's account.
        accountServiceClient.deductBalance(request.getSenderAccountNumber() , request.getAmount());

        // Create TransactionEntity and prepare all transaction details
        // before saving the transaction into the database.
        TransactionEntity transaction = new TransactionEntity();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setDescription(request.getDescription());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        TransactionEntity savedTransaction = transactionRepository.save(transaction);

        log.info("Transaction saved as processing : {} " , savedTransaction.getId());

        // Create an event object containing transaction details
        // that will be published to Kafka for the next SAGA step.
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        // SAGA Step 2: Publish transaction event to Kafka
        // so the next service can consume it and continue the SAGA flow.
        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId() , event);

        log.info("SAGA step : 2  - TransactionInitiatedEvent published : {} ",savedTransaction.getId());
        return mapToResponse(savedTransaction);
    }

    private TransactionResponse mapToResponse(TransactionEntity savedTransaction) {

        TransactionResponse response = new TransactionResponse();
        response.setId(savedTransaction.getId());
        response.setSenderAccountNumber(savedTransaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(savedTransaction.getReceiverAccountNumber());
        response.setAmount(savedTransaction.getAmount());
        response.setType(savedTransaction.getType());
        response.setStatus(savedTransaction.getStatus());
        response.setDescription(savedTransaction.getDescription());
        response.setReferenceNumber(savedTransaction.getReferenceNumber());
        response.setFailureReason(savedTransaction.getFailureReason());
        response.setCompletedAt(savedTransaction.getCompletedAt());
        response.setCreatedAt(savedTransaction.getCreatedAt());
        return  response;
    }


    public TransactionResponse getTransaction(String transactionId) {

        TransactionEntity response = transactionRepository.findById(transactionId)
                                                            .orElseThrow(() -> new RuntimeException(
                                                                    "Transaction Not Found" + transactionId
                                                            ));
        return mapToResponse(response);
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public TransactionResponse verify(String transactionId, String otp) {

        log.info("OTP verification for the transaction : {} " , transactionId);

        TransactionEntity transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException(
                                                                                                "Transaction Not found "+ transactionId
                                                                                                ));

        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);  // getting the stored otp from redis db


        if(storedOtp == null){
            // OTP expired
            log.warn("Otp expired for transaction : {} " , transactionId);
            compensateTransaction(transaction, "OTP Expired - transaction cancelled and amount refunded ");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            // BLOCK account and REFUND
            log.warn("Wrong Otp - blocking account and refunding : {} ", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction , "Wrong OTP entered - transaction cancelled " +
                                                    "account blocked for security ");
            return mapToResponse(transaction);
        }

        // OTP correct - complete transaction

        log.info("OTP verified - completing transaction : {} " , transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);



    }

    private void completeTransaction(TransactionEntity transaction) {

        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(transaction.getId() ,
                                                                                  transaction.getSenderAccountNumber(),
                                                                                    transaction.getReceiverAccountNumber(),
                                                                                    transaction.getAmount() ,
                                                                                    transaction.getDescription());

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC , transaction.getId() , completedEvent);
        log.info("SAGA COMPLETED - transaction {} completed " , transaction.getId());


    }











    private void blockAccountAndCompensate(TransactionEntity transaction, String reason) {

        // publish fraud.detected -> account service will be blocked
        Map<String ,Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transaction" , transaction.getId());
        fraudEvent.put("accountNumber" , transaction.getSenderAccountNumber());
        fraudEvent.put("reason" , reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC , transaction.getSenderAccountNumber() , fraudEvent);
        log.warn("fraud.detected published - account : {} will be blocked , kindly contact to the bank " , transaction.getSenderAccountNumber());

        // SAGA COMPENSATION - refund sender
        compensateTransaction(transaction,reason);





    }

    private void compensateTransaction(TransactionEntity transaction, String reason) {

        log.warn("SAGA COMPENSATION - refunding : {} amount : {}  " , transaction.getSenderAccountNumber() , transaction.getAmount());

        // CREDIT money back to the sender Synchronously
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber() , transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + "-SAGA compensation executed ,amount refunded at " + LocalDateTime.now());
        transactionRepository.save(transaction);

        // publish refund event : notification service will alert user
        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber" , transaction.getSenderAccountNumber());
        refundEvent.put("amount" , transaction.getAmount());
        refundEvent.put("reason" , reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC , transaction.getId() , refundEvent);

        log.info("SAGA COMPENSATION COMPLETED - {}  refund to {} ", transaction.getAmount() , transaction.getSenderAccountNumber());



    }

    public void processCleanResult(String transactionId) {
        TransactionEntity transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction Not Found" + transactionId
                ));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not Processing - skipping " , transactionId);
            return;
        }

        completeTransaction(transaction);

    }
}
