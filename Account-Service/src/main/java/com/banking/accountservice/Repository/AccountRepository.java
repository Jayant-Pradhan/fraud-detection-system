package com.banking.accountservice.Repository;

import com.banking.accountservice.Entity.AccountEntity;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<AccountEntity,String> {
    boolean existsByMail( String mail);
    boolean existsByAccountNumber(String accountNumber);
    Optional<AccountEntity> findByAccountNumber(String accountNumber);
}
