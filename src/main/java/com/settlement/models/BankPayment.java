package com.settlement.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BankPayment {
    private String orderId;
    private String status;
    @Override
    public String toString() {
        return "BankPayment{orderId='" + orderId + "', status='" + status + "'}";
    }
}
