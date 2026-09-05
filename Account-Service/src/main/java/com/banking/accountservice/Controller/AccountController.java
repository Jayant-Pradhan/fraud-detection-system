package com.banking.accountservice.Controller;

import com.banking.accountservice.DTO.AccountRequest;
import com.banking.accountservice.DTO.AccountResponse;
import com.banking.accountservice.Service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@Controller
@RequestMapping("/api/v1/account")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    // create new  account
    @PostMapping("/create")
    public ResponseEntity<AccountResponse> createNewAccount(@Valid @RequestBody AccountRequest request){
        return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createNewAccount(request));
    }

    // Get Account details
    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable String accountNumber){
        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    // Get Balance
    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getBalance(@PathVariable String accountNumber){
        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    // Block account
    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> blockAccount(@PathVariable String accountNumber){
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account Blocked Successfully");
    }

    /**
            SAGA STEP :  1 --> Deduct Balance
            Called by Transaction Service when transfer is initiated
     */

    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductBalance(@PathVariable String accountNumber , @RequestParam BigDecimal amount){
        accountService.deductBalance(accountNumber , amount);
        return ResponseEntity.ok("BALANCE  DEDUCTED  SUCCESSFULLY");
    }

    /*
       **  SAGA STEP : 4 --> Compensation transaction endpoint
                ** Agar SAGA ke beech mein koi step fail ho jata hai,
                        to pehle successfully complete hue steps ko undo/reverse karne ke liye jo endpoint hot hai,
                                use compensation endpoint kehte hai.
       **  CALLED BY TRANSACTION SERVICE IN TWO SCENARIOS :
                    1 . FRAUD DETECTED --> Refund money to Sender
                    2 . Transaction completed  --> Credit to Receiver
    */

    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(@PathVariable String accountNumber , @RequestParam BigDecimal amount){
        accountService.creditBalance(accountNumber,amount);
        return ResponseEntity.ok("BALANCE CREDITED SUCCESSFULLY");

    }












}
