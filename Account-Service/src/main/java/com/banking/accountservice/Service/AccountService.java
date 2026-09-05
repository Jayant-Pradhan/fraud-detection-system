package com.banking.accountservice.Service;

import com.banking.accountservice.DTO.AccountRequest;
import com.banking.accountservice.DTO.AccountResponse;
import com.banking.accountservice.Entity.AccountEntity;
import com.banking.accountservice.Entity.AccountStatus;
import com.banking.accountservice.Entity.AccountType;
import com.banking.accountservice.Repository.AccountRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {
    private final AccountRepository accountRepository;
    private static final SecureRandom secureRandom = new SecureRandom();


    public AccountResponse createNewAccount(AccountRequest request) {
        log.info("Creating Account for : {} " , request.getMail());

        if(accountRepository.existsByMail(request.getMail())){
            throw new RuntimeException("Account is already existed in this email : " + request.getMail());

        }
        AccountEntity account = new AccountEntity();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setMail(request.getMail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());
        account.setAccountNumber(generateAccountNumber());
        account.setDailyTransactionLimit(request.getAccountType() == AccountType.SAVING ? new BigDecimal(100000) : new BigDecimal(50000));

        AccountEntity savedAccount = accountRepository.save(account);
        log.info("Account created successfully : {} " , savedAccount.getAccountNumber());

        return mapToResponse(savedAccount);
    }

    // generate 12 digit unique account number
    private String generateAccountNumber() {
        String accountNumber;

        do{
            long number = secureRandom.nextLong(1_000_000_000L);
            accountNumber = String.format("%012d",number);
        }while(accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(AccountEntity savedAccount) {
        AccountResponse response = new AccountResponse();
        response.setId(savedAccount.getId());
        response.setAccountHolderName(savedAccount.getAccountHolderName());
        response.setAccountNumber(savedAccount.getAccountNumber());
        response.setMail(savedAccount.getMail());
        response.setPhone(savedAccount.getPhone());
        response.setAccountType(savedAccount.getAccountType());
        response.setStatus(savedAccount.getStatus());
        response.setBalance(savedAccount.getBalance());
        response.setDailyTransactionLimit(savedAccount.getDailyTransactionLimit());
        response.setCreatedAt(savedAccount.getCreatedAt());
        return response;

    }


    public AccountResponse getAccount(String accountNumber) {
        AccountEntity account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account Not Found"));
        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        AccountEntity account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account Not Found"));
        return account.getBalance();
    }


    /*
            Block account -- called by Fraud detection service via Kafka
     */
    public void blockAccount(String accountNumber) {
        log.info("Blocking account : {} ", accountNumber);
        AccountEntity account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account Not Found"));
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked : {} " , accountNumber);
    }

    /**
     *  Deduct Balance from sender account
     *  Called by transaction Service
     * @param accountNumber
     * @param amount
     */

    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting balance {} from account : {}  " , amount , accountNumber);
        AccountEntity account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account Not Found"));
        if(account.getStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException("account is inactive:  {} " + accountNumber);
        }
        if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient balance : {} " + accountNumber);
        }
        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance Update . New Balance : {} " , account.getBalance());
    }

    /**
     * Credit balance
     * Called By Transaction Service via Kafka
     * @param accountNumber
     * @param amount
     */
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting {} to account : {} " ,amount , accountNumber);
        AccountEntity account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account Not Found"));
        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balance credited . New Balance : {} " , account.getBalance());
    }
}
