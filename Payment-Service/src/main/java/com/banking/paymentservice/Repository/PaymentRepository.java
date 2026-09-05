package com.banking.paymentservice.Repository;

import com.banking.paymentservice.Entity.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity , String> {

    Optional<PaymentEntity> findByRazorpayOrderId(String orderId);


}
