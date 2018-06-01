package com.kelles.crawler.crawler.parser;

import com.kelles.crawler.crawler.analysis.CommonAnalysis;
import com.kelles.crawler.crawler.setting.Constant;
import com.kelles.crawler.crawler.setting.Setting;
import com.kelles.crawler.crawler.util.Logger;
import com.kelles.crawler.crawler.util.Util;

import java.io.UnsupportedEncodingException;
import java.net.*;

public class TenderParser extends AbstractParser {

    public static void main(String[] args) {
        TenderParser tenderParser = null;
        try {
            tenderParser = new TenderParser();
            tenderParser.addUrl("http://www.chinazbcgou.com.cn/");
            tenderParser.start();
        } finally {
            tenderParser.close();
        }

    }

    @Override
    protected void onCrawlingUrl(String url) {
        if (Util.isEmpty(url)) return;
        try {
            Logger.log("[开始分析]" + URLDecoder.decode(url, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        if (isHomePage(url)) {
            onCrawlingHomePage(url);
        }
    }

    /**
     * 爬取首页
     *
     * @param url
     */
    protected void onCrawlingHomePage(String url) {
        //获取html文本
        String html = CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
        if (html == null) {
            Logger.log("[获取html失败]" + url);
            return;
        }
        //
    }

    protected boolean isHomePage(String urlStr) {
        if (Util.isEmpty(urlStr)) return false;
        try {
            URL url = new URL(urlStr);
            return (Setting.URL_TENDER_HOMEPAGE.equals(url.getHost())
                    && ("".equals(url.getPath()) || Constant.PATH_SEPERATOR.equals(url.getPath())));
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        }
    }
}
