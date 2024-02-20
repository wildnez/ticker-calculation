package com.trade_calc.demo.domain;

import org.joda.time.DateTime;
import java.io.Serializable;

public class Ticker implements Serializable {

	private int price;
	private String ticker;
	private String source;
	private DateTime tradeTime;

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public DateTime getTradeTime() {
		return tradeTime;
	}

	public void setTradeTime(DateTime tradeTime) {
		this.tradeTime = tradeTime;
	}

	public int getPrice() {
		return price;
	}

	public void setPrice(int price) {
		this.price = price;
	}

	public  String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public Ticker(String ticker, int price, String source, String time_string) {
		this.ticker = ticker;
		this.price = price;
		this.source = source;
		this.tradeTime = DateTime.parse(time_string);
	}

	@Override
	public String toString() {
		return "Ticker: "+ ticker + ", Value: "+price+", Time: "+tradeTime;
	}
}
