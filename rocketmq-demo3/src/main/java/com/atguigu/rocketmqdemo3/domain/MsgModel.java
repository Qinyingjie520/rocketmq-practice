package com.atguigu.rocketmqdemo3.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MsgModel {
    private String orderSn;
    private Integer userId;
    private String desc;
}
