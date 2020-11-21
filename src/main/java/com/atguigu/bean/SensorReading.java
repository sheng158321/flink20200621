package com.atguigu.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long s;
    private Double temp;

    @Override
    public String toString() {
        return id + "," + s + "," + temp;
    }
}
