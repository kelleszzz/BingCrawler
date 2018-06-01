package com.kelles.crawler.crawler.parser;

import com.kelles.crawler.crawler.bean.CrawlUrl;
import com.kelles.crawler.crawler.database.UrlsDbManager;
import com.kelles.crawler.crawler.download.DownloadPool;
import com.kelles.crawler.crawler.setting.Constant;
import com.kelles.crawler.crawler.setting.Setting;
import com.kelles.crawler.crawler.util.Logger;
import com.kelles.crawler.crawler.util.RemoteDriver;
import com.kelles.crawler.crawler.util.Util;
import com.sleepycat.je.OperationStatus;
import org.openqa.selenium.NoSuchSessionException;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Scanner;

/**
 * 整合了UrlsDbManager、DownloadPool和RemoteDriver的基础Parser
 * 通过实现几个抽象方法来完成流程
 */
public abstract class AbstractParser implements Closeable {

    protected UrlsDbManager urlsDbManager = null;
    protected RemoteDriver remoteDriver = null;
    protected DownloadPool downloadPool = null;
    protected Scanner scanner = new Scanner(System.in);

    /**
     * 处理控制台输入
     *
     * @param userInput
     * @return
     */
    protected int handleUserInput(String userInput,String nextUrl) {
        if ("exit".equals(userInput)) {
            return Setting.STATUS_USERINPUT_EXIT;
        } else if ("p".equals(userInput)) {
            settleUrl(nextUrl);
            return Setting.STATUS_USERINPUT_SUCCESS;
        } else if ("db".equals(userInput)) {
            for (; ; ) {
                Logger.log(Setting.MENU_DB);
                try {
                    userInput = scanner.nextLine();
                    if ("1".equals(userInput)) urlsDbManager.describe();
                    else if ("2".equals(userInput)) downloadPool.describe();
                    else if ("3".equals(userInput)) downloadPool.getManager().clearDb();
                    else break;
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            return Setting.STATUS_USERINPUT_SUCCESS;
        } else if ("auto".equals(userInput)) {
            Logger.log("输入自动爬取的链接数");
            userInput = scanner.nextLine();
            try {
                int auto = Integer.parseInt(userInput);
                for (int i = 0; i < auto; i++) {
                    if ((nextUrl = urlsDbManager.getNext()) != null) {
                        try {
                            onCrawlingUrl(nextUrl);
                        } catch (NoSuchSessionException e0) {
                            //这是紧急异常,表示浏览器会话失效
                            Logger.log("[异常]" + Util.getSystemTime());
                            e0.printStackTrace();
                            break;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else break;
                }
            } catch (NumberFormatException e1) {
                Logger.log("输入错误");
            }
            return Setting.STATUS_USERINPUT_SUCCESS;
        } else if ("download".equals(userInput)) {
            downloadPool.tryStart();
        } else {
            try {
                onCrawlingUrl(nextUrl);
            } catch (NoSuchSessionException e0) {
                //这是紧急异常,表示浏览器会话失效
                Logger.log("[异常]" + Util.getSystemTime());
                e0.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return Setting.STATUS_USERINPUT_SUCCESS;
        }
        return Setting.STATUS_USERINPUT_EXIT;
    }

    public void start(){
        menu:for (;; ) {
            Logger.log(Setting.MENU);
            String userInput = scanner.nextLine();
            String nextUrl = urlsDbManager.getNext();
            try {
                Logger.log("[即将分析]" + URLDecoder.decode(nextUrl, "utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            switch (handleUserInput(userInput,nextUrl)){
                case Setting.STATUS_USERINPUT_SUCCESS: {
                    continue menu;
                }
                case Setting.STATUS_USERINPUT_EXIT:{
                    return;
                }
            }
        }
    }

    protected void setup() {
        try {
            Logger.log("[初始化]Initializing DownloadPool & UrlsDbManager");
            downloadPool = new DownloadPool(Setting.BING_PARSER_DOWNLOADPOOL_PATH);
            downloadPool.setMaxThreads(5); //同时进行5个下载任务
            urlsDbManager = new UrlsDbManager(Setting.BING_PARSER_URLSDB_PATH);
            urlsDbManager.setMaxDepth(3); //深度
            RemoteDriver.startService();
            remoteDriver = new RemoteDriver(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        downloadPool.close();
        urlsDbManager.close();
        remoteDriver.stopService();
        RemoteDriver.stopService();
    }

    /**
     * 开始爬取指定url
     *
     * @param url
     */
    protected abstract void onCrawlingUrl(String url);

    /**
     * 添加url
     *
     * @param url
     */
    protected void addUrl(String url) {
        if (Util.isEmpty(url)) return;
        if (urlsDbManager.putUrl(url) == OperationStatus.SUCCESS) {
            Logger.log("[添加链接]" + url); //
            urlsDbManager.updateWeight(url, CrawlUrl.DEFAULT_WEIGHT + 10);
        }
    }

    /**
     * 从todoUrls中移除,并添加至uniUrls
     *
     * @param url
     */
    protected void settleUrl(String url) {
        if (Util.isEmpty(url)) return;
        urlsDbManager.settleUrl(url, 0);
    }

    /**
     * 获取下一个待爬取的url
     */
    protected String getNextUrl() {
        return urlsDbManager.getNext();
    }

    public AbstractParser() {
        setup();
    }
}
