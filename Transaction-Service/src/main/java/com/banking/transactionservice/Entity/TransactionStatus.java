package com.banking.transactionservice.Entity;

/*
        Transaction lifeCycle flow :
            PENDING -> PROCESSING -> COMPLETED (Clean transaction )
                                    -> PENDING_VERIFICATION(suspicious detected)
                                                -> COMPLETED(verified)
                                                -> FLAGGED (SAGA REFUND )
                                    -> FAILED
                                     -> FLAGGED

 */

public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED


}
