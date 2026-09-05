package com.banking.transactionservice.Repository;

import com.banking.transactionservice.Entity.TransactionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TransactionRepository extends JpaRepository<TransactionEntity ,String> {

    List<TransactionEntity> findBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);
}
