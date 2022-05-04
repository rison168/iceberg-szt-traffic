package com.rison.szt.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Program: iceberg-szt-traffic
 * @Description: 深圳通刷卡数据实体类
 * @Author: RISON
 * @Create: 2022-05-05
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SztDataEntity {
    private String deal_date;
    private String close_date;
    private String card_no;
    private String deal_value;
    private String deal_type;
    private String company_name;
    private String car_no;
    private String station;
    private String conn_mark;
    private String deal_money;
    private String equ_no;
}
