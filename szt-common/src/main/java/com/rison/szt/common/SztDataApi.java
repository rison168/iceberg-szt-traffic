package com.rison.szt.common;


import cn.hutool.core.io.FileUtil;
import cn.hutool.http.HttpUtil;
/**
 * @Program: iceberg-szt-traffic
 * @Description: 获取深圳通刷卡数据
 * @Author: RISON
 * @Create: 2022-05-05
 */
public class SztDataApi{
    /**
     * 数据保存路径
     */
    final static String DATA_PATH = "/Users/rison/data/tmp-data/szt-data/szt-data.json";
    /**
     * 我的 web: https://opendata.sz.gov.cn 申请的appKey
     */
    final static String MY_APP_KEY = "xxxx";
    public static void main(String[] args) {
        for (int i = 1; i <= 1337; i++){
            String value = HttpUtil.get("https://opendata.sz.gov.cn/api/29200_00403601/1/service.xhtml?page=" + i + "&rows=1000&appKey=" + MY_APP_KEY);
            FileUtil.appendUtf8String(value + "\n", DATA_PATH);
            System.out.println("正在获取 第【" + i + "】页数据...");
        }
        //检验数据完整性
        if (FileUtil.readUtf8Lines(DATA_PATH).size() == 1337){
            System.out.println("====数据保存完整=====");
        }


    }
}
