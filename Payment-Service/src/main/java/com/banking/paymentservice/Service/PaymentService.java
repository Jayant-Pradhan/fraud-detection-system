package com.banking.paymentservice.Service;

import com.banking.paymentservice.DTO.PaymentRequest;
import com.banking.paymentservice.DTO.PaymentResponse;
import com.banking.paymentservice.Entity.PaymentEntity;
import com.banking.paymentservice.Entity.PaymentStatus;
import com.banking.paymentservice.Repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.banking.paymentservice.Entity.PaymentStatus.CREATED;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository repository;
    private final KafkaTemplate<String,Object> kafkaTemplate;


    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;


    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";









    /**
     *      Create Razorpay payment order
     *  FLOW :
     *      1 .Create Order in Razorpay
     *      2 . save payment record in db
     *      3 . return order details to frontend
     *      4 . frontend show Razorpay check out
     *      5 . user pays
     *      6 . Razorpay calls webhook
     *
     *
     * @param request
     * @return
     */

    public PaymentResponse createPaymentOrder(@Valid PaymentRequest request) throws RazorpayException {

        log.info("Creating payment order for account : {} amount : {} " , request.getAccountNumber() , request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId , keySecret);

        // converted amount
        int convertedAmount = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount" , convertedAmount);
        orderRequest.put("currency" , "USD/INR");
        orderRequest.put("receipt","rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString().replace("-" , "").substring(0,10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created : {} " , razorpayOrder.get("id").toString());


        // save Payment record
        PaymentEntity payment = new PaymentEntity();

        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("USD/INR");
        payment.setStatus(CREATED);
        payment.setDescription(request.getDescription());

        PaymentEntity savedPayment = repository.save(payment);

        return new PaymentResponse(savedPayment.getId(),razorpayOrder.get("id").toString() ,request.getAmount(),"USD/INR" , keyId,"CREATED");



    }


    public void handleWebhook(Map<String, Object> payload) {

        log.info("Received Razorpay webhook : {} " , payload.get("event"));

        String event = (String) payload.get("event");

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }
        else if ("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }


    }




    private void handlePaymentSuccess(Map<String, Object> payload) {

        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            PaymentEntity payment = repository.findByRazorpayOrderId(orderId).orElseThrow(()-> new RuntimeException(
                                                                                            "Payment not found for order : " + orderId
            ));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            repository.save(payment);

            // publish payment completed event

            Map<String,Object> event = new HashMap<>();
            event.put("paymentId" , payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount" , payment.getAmount());
            event.put("razorpayPayment" , paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,payment.getId() , event);
            log.info("payment completed : {}",payment.getId());

        }
        catch(Exception e){
            log.error("error handling payment success : {} " , e.getMessage());

        }
    }
    private void handlePaymentFailure(Map<String, Object> payload) {

        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            PaymentEntity payment = repository.findByRazorpayOrderId(orderId).orElseThrow(()-> new RuntimeException(
                    "Payment not found for order : " + orderId
            ));


            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay ");
            repository.save(payment);

            // publish payment completed event

            Map<String,Object> event = new HashMap<>();
            event.put("paymentId" , payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount" , payment.getAmount());
            event.put("reason" , "payment failed via Razorpay ");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId() , event);
            log.warn("Payment failed : {} " , payment.getId());


        }
        catch (Exception e){

            log.error("error handling payment failure : {}" , e.getMessage());

        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {

        Map<String,Object> entity = (Map<String , Object>) payload.get("payload");

        Map<String,Object> paymentWrapper = (Map<String , Object>) entity.get("PaymentEntity");

        return (Map<String,Object>) paymentWrapper.get("entity");

    }
}
