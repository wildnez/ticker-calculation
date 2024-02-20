package com.trade_calc.demo.util;

import org.joda.time.DateTime;

import java.io.Serializable;

public class TradeAccumulator implements Serializable {

    private long price_this;
    private long price_other;
    private DateTime time;

    public void accumulate(long value) {
        price_other = value;
        //price = price > value ? price : value;
    }

    public void combine(TradeAccumulator other) {
        //do nothing
    }

    public long export() {
        return price_this < price_other ? price_this : price_other;
    }

}
