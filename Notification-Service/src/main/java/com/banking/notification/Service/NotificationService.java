package com.banking.notification.Service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(@Payload Map<String,Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String transactionId = (String) payload.get("transactionId");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber , "TRANSACTION VERIFICATION REQUIRED" , String.format("Suspicious activity detected on you account . " +
                                                                            "Reason : %s " +
                                                                            "A Transaction of %s is pending verification " +
                                                                            "Your Otp is : %s. valid for 5 minute ." +
                                                                            "If this wasn't you - ignore this message." ,
                                                                            otp,transactionId , amount,reason


            ));

        }
        catch (Exception e){
            log.error("Error sending otp notification : {} ", e.getMessage());
        }

    }


    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){
        try{
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            //DEBIT alert

            sendAlert(senderAccount , "DEBIT ALERT" , String.format("%s debited from account %s" , amount, senderAccount));

            // CREDIT alert
            sendAlert(receiverAccount , "CREDIT ALERT" , String.format("%s credited from account %s" , amount, receiverAccount));



        }
        catch(Exception e){
            log.error("error sending transaction notification :{} ", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber , "SUSPICIOUS ACTIVITY DETECTED" , String.format(
                                    "Your account %s has been blocked." +
                                    "Reason : %s"+
                                    "please contact your bank immediately.", accountNumber,reason
            ));
        }
        catch(Exception e){
            log.error("Error sending fraud alert : {} ", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefund(@Payload Map<String,Object> payload){
       try{
           String senderAccount = (String) payload.get("senderAccountNumber");
           String amount = payload.get("amount").toString();
           String reason = (String) payload.get("reason");

           sendAlert(senderAccount , "REFUND PROCESSED" , String.format("Your transaction of %s was cancelled." +
                           "Reason : %s " +
                           "%s has been refunded to account %s.",
                   amount,reason,amount ,senderAccount
           ));
       }
       catch (Exception e){
           log.error("error sending refund notification : {} " , e.getMessage());
       }

    }
    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String,Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            sendAlert(accountNumber , "PAYMENT SUCCESSFUL" ,String.format("Payment of %s completed" + "RazorPay ID : %s" , amount,payload.get("razorpayPaymentId")));

        }
        catch (Exception e){
            log.error("error sending payment notification : {} " , e.getMessage());
        }
    }
    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String,Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber , "PAYMENT FAILED" , String.format("your payment of %s count not be processed." + "please try again or contact support " , amount));
        }
        catch (Exception e){
            log.error("error sending payment failure notification  : {}",e.getMessage());
        }


    }





    private void sendAlert(String accountNumber, String subject, String message) {

        log.info("----------------------------------------------------------");
        log.info("Account : {} " , accountNumber);
        log.info("Subject : {} " , subject);
        log.info("Message : {} " , message);
        log.info("------------------------------------------------------------");

    }
}
