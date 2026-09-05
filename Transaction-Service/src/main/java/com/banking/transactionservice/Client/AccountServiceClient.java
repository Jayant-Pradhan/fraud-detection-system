package com.banking.transactionservice.Client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;

@FeignClient(name = "Account-Service" , url = "${Account.Service.url}")
public interface AccountServiceClient {

    @PutMapping("/api/v1/account/{accountNumber}/deduct")
    String deductBalance(@PathVariable String accountNumber , @RequestParam BigDecimal amount);


    @PutMapping("/api/v1/account/{accountNumber}/credit")
    String creditBalance(@PathVariable String accountNumber , @RequestParam BigDecimal amount);
}
