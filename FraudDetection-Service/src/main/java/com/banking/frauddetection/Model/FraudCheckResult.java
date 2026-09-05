package com.banking.frauddetection.Model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class FraudCheckResult {

    private boolean fraud;
    private String reason;

}
