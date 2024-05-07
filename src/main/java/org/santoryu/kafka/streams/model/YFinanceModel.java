package org.santoryu.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.List;


// {"ticker": "AAPL", "dateTime": "2023-07-03T09:30:00+04:00", "open": 193.77999877929688, "high": 193.8800048828125, "low": 192.8000030517578, "close": 192.9149932861328, "adjClose": 192.9149932861328, "volume": 12730490}

// using Lombok library can reduce the verbosity of the model
public class YFinanceModel {
    private String ticker;
    @JsonProperty("datetime")
    private Date dateTime;
    private double open;
    private double high;
    private double low;
    private double close;
    @JsonProperty("adjclose")
    private double adjClose;
    private double volume;
    private List<Double> lastTenPrices;
    private Double moving_avg_10;
    private Double moving_avg_20;
    private Double moving_avg_50;

    // Jackson requires a no-argument (default) constructor to instantiate the object before setting the fields via setters
    public YFinanceModel() {
    }

    public YFinanceModel(Builder builder) {
        this.ticker = builder.ticker;
        this.dateTime = builder.dateTime;
        this.open = builder.open;
        this.high = builder.high;
        this.low = builder.low;
        this.close = builder.close;
        this.adjClose = builder.adjClose;
        this.volume = builder.volume;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(YFinanceModel yFinanceModel) {
        return new Builder(yFinanceModel);
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public double getAdjClose() {
        return adjClose;
    }

    public void setAdjClose(double adjClose) {
        this.adjClose = adjClose;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public List<Double> getLastTenPrices() {
        return lastTenPrices;
    }

    public void setLastTenPrices(List<Double> lastTenPrices) {
        this.lastTenPrices = lastTenPrices;
    }

    public Double getMoving_avg_10() {
        return moving_avg_10;
    }

    public void setMoving_avg_10(Double moving_avg_10) {
        this.moving_avg_10 = moving_avg_10;
    }

    public Double getMoving_avg_20() {
        return moving_avg_20;
    }

    public void setMoving_avg_20(Double moving_avg_20) {
        this.moving_avg_20 = moving_avg_20;
    }

    public Double getMoving_avg_50() {
        return moving_avg_50;
    }

    public void setMoving_avg_50(Double moving_avg_50) {
        this.moving_avg_50 = moving_avg_50;
    }

    @Override
    public String toString() {
        return "YFinanceModel{" +
                "ticker=" + ticker +
                ", dateTime=" + dateTime +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", adjClose=" + adjClose +
                ", volume=" + volume +
                ", lastTenPrices=" + lastTenPrices +
                ", moving_avg_10=" + moving_avg_10 +
                ", moving_avg_20=" + moving_avg_20 +
                ", moving_avg_50=" + moving_avg_50 +
                '}';
    }

    public static class Builder {
        private String ticker;
        private Date dateTime;
        private double open;
        private double high;
        private double low;
        private double close;
        private double adjClose;
        private double volume;
        private List<Double> lastTenPrices;
        private Double moving_avg_10;
        private Double moving_avg_20;
        private Double moving_avg_50;

        private Builder() {
        }

        private Builder(YFinanceModel yFinanceModel) {
            this.ticker = yFinanceModel.getTicker();
            this.dateTime = yFinanceModel.getDateTime();
            this.open = yFinanceModel.getOpen();
            this.high = yFinanceModel.getHigh();
            this.low = yFinanceModel.getLow();
            this.close = yFinanceModel.getClose();
            this.adjClose = yFinanceModel.getAdjClose();
            this.volume = yFinanceModel.getVolume();
            this.lastTenPrices = yFinanceModel.getLastTenPrices();
            this.moving_avg_10 = yFinanceModel.getMoving_avg_10();
            this.moving_avg_20 = yFinanceModel.getMoving_avg_20();
            this.moving_avg_50 = yFinanceModel.getMoving_avg_50();
        }

        public YFinanceModel build() {
            return new YFinanceModel(this);
        }

        public Builder ticker(String val) {
            this.ticker = val;
            return this;
        }

        public Builder dateTime(Date val) {
            this.dateTime = val;
            return this;
        }

        public Builder open(double val) {
            this.open = val;
            return this;
        }

        public Builder high(double val) {
            this.high = val;
            return this;
        }

        public Builder low(double val) {
            this.low = val;
            return this;
        }

        public Builder close(double val) {
            this.close = val;
            return this;
        }

        public Builder adjClose(double val) {
            this.adjClose = val;
            return this;
        }

        public Builder volume(double val) {
            this.volume = val;
            return this;
        }

        public Builder lastTenPrices(List<Double> val) {
            this.lastTenPrices = val;
            return this;
        }

        public Builder moving_avg_10(Double val) {
            this.moving_avg_10 = val;
            return this;
        }

        public Builder moving_avg_20(Double val) {
            this.moving_avg_20 = val;
            return this;
        }

        public Builder moving_avg_50(Double val) {
            this.moving_avg_50 = val;
            return this;
        }

    }

}

