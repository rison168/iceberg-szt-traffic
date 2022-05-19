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
    /**
     * 交易时间
     */
    private String deal_date;
    /**
     * 结算时间
     */
    private String close_date;
    /**
     * 卡号
     */
    private String card_no;
    /**
     * 交易金额
     */
    private String deal_value;
    /**
     * 交易类型
     */
    private String deal_type;
    /**
     * 公司名称，线名
     */
    private String company_name;
    /**
     * 列车号
     */
    private String car_no;
    /**
     * 站名
     */
    private String station;
    /**
     * 联程标记
     */
    private String conn_mark;
    /**
     * 实收金额
     */
    private String deal_money;
    /**
     * 闸机号
     */
    private String equ_no;
}
