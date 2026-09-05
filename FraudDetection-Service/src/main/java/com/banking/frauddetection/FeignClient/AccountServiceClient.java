package com.banking.frauddetection.FeignClient;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.math.BigDecimal;

@FeignClient(name= "Account-Service" , url = "${Account.Service.url}")
public interface AccountServiceClient {
    @GetMapping("/api/v1/account/{accountNumber}/balance")
    BigDecimal getBalance(@PathVariable String accountNumber);
}
