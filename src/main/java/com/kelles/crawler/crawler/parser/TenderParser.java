package com.kelles.crawler.crawler.parser;

import com.google.gson.Gson;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import com.kelles.crawler.crawler.analysis.CommonAnalysis;
import com.kelles.crawler.crawler.analysis.Simhash;
import com.kelles.crawler.crawler.bean.CrawlUrl;
import com.kelles.crawler.crawler.bean.NLPException;
import com.kelles.crawler.crawler.setting.Constant;
import com.kelles.crawler.crawler.setting.Setting;
import com.kelles.crawler.crawler.util.Logger;
import com.kelles.crawler.crawler.util.Util;
import com.kelles.fileserver.fileserversdk.sdk.FileServerSDK;
import com.kelles.userserver.userservercloud.userserversdk.sdk.UserServerSDK;
import com.sleepycat.je.OperationStatus;
import okhttp3.HttpUrl;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Assert;
import org.openqa.selenium.TimeoutException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TenderParser extends AbstractParser {

    FileServerSDK fileServerSDK;
    UserServerSDK userServerSDK;
    Gson gson = new Gson();
    final static String MESSAGE_TITLE = "title";

    public static void main(String[] args) {
        TenderParser tenderParser = null;
        try {
            tenderParser = new TenderParser();
//            tenderParser.addHomePage(Setting.URL_TENDER_HOMEPAGE);
            tenderParser.menu();
        } finally {
            if (tenderParser != null) {
                tenderParser.close();
            }
        }
    }

    @Override
    protected void onCrawlingUrl(String url) {
        if (Util.isEmpty(url)) return;
        Logger.log("[开始分析]" + url, true);
        if (isHomePage(url)) {
            //[http://www.chinazbcgou.com.cn]
            onCrawlingHomePage(url);
        } else if (isDisplayPage(url)) {
            //[http://www.chinazbcgou.com.cn/display.php?id=3095492]
            onCrawlingDisplayPage(url);
        } else if (isSearchPage(url)) {
            onCrawlingSearchPage(url);
        } else if (isResultPage(url)) {
            onCrawlingResultPage(url);
        } else {
            OperationStatus status = urlsDbManager.updateWeightByRelativeValue(url, -1);
            if (!OperationStatus.SUCCESS.equals(status)) {
                Logger.log("[降低优先级失败]" + url, true);
            }
        }
    }

    protected void onCrawlingResultPage(String url) {
        if (Util.isEmpty(url) || !isResultPage(url)) return;
        OperationStatus status = null;
        try {
            //获取html文本
            String html = CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
            if (html == null) {
                Logger.log("[无法访问]" + url, true);
                urlsDbManager.updateWeightByRelativeValue(url, -1); //自降优先级
                return;
            } else {
                Logger.log("[分析结果页]" + url, true);
                urlsDbManager.updateWeightByRelativeValue(url, -1); //自降优先级,防止过程出错而循环爬取
            }
            //获取标题
            String title = urlsDbManager.getMessage(url, TenderParser.MESSAGE_TITLE);
            if (title == null) {
                Logger.log("[获取标题失败]" + url);
                return;
            }
            //保存html文本至本地文件
            Path dir = Paths.get(Setting.TENDER_PARSER_RESULT_DIRECTORY);
            if (!Files.isDirectory(dir)) {
                Files.createDirectories(dir);
            }
            String fileName = Setting.PREFIX_RESULT + title + "_" + UUID.randomUUID();
            fileName = CommonAnalysis.filterFileName(fileName, "html");
            Path path = Paths.get(dir.toString(), fileName);
            if (!Files.isRegularFile(path)) {
                Files.createFile(path);
            }
            CommonAnalysis.textToFile(Util.htmlFormatting(html), dir.toString(), path.getFileName().toString());
            //上传html文本至FileServer&UserServer
            Util.uploadFileAndGrant(path, fileServerSDK, userServerSDK, gson);
            //从urlsDbManager中移除
            status = urlsDbManager.settleUrl(url, Setting.STATUS_URL_SUCCESS);
            if (!OperationStatus.SUCCESS.equals(status)) {
                Logger.log("[移除URL失败]" + url, true);
            }
        } catch (TimeoutException e) {
            onTimeout(url);
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO 在搜索引擎中查找内容
     *
     * @param url
     */
    protected void onCrawlingSearchPage(String url) {
        if (Util.isEmpty(url) || !isSearchPage(url)) return;
        OperationStatus status = null;
        try {
            //获取标题
            String title = null;
            try {
                URL urlSrc = new URL(url);
                String[] querys = urlSrc.getQuery().split("&");
                Assert.assertTrue(querys != null);
                for (String query : querys) {
                    Pattern pattern = Pattern.compile("wd=(.+?)");
                    Matcher matcher = pattern.matcher(query);
                    if (matcher.matches()) {
                        title = matcher.group(1);
                        title = URLDecoder.decode(title, Setting.DEFAULT_CHARSET.displayName());
                    }
                }
            } catch (MalformedURLException e) {
                e.printStackTrace();
                return;
            }
            //获取html文本
            String html = CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
            if (html == null) {
                Logger.log("[无法访问]" + url, true);
                urlsDbManager.updateWeightByRelativeValue(url, -1); //自降优先级
                return;
            } else {
                Logger.log("[分析搜索页]" + title);
                urlsDbManager.updateWeightByRelativeValue(url, -1); //自降优先级,防止过程出错而循环爬取
            }
            //获取链接,判断相似度,并添加相关链接至urlsDbManager
            onAnalyzeSearchPage(title, html, url);
            //从urlsDbManager中移除
            status = urlsDbManager.settleUrl(url, Setting.STATUS_URL_SUCCESS);
            if (!OperationStatus.SUCCESS.equals(status)) {
                Logger.log("[移除URL失败]" + url, true);
            }
        } catch (TimeoutException e) {
            onTimeout(url);
            return;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * 分析搜索引擎搜索页,获取搜索结果链接
     * 获取链接,判断相似度,并添加相关链接至urlsDbManager
     *
     * @param title
     * @param html
     */
    protected void onAnalyzeSearchPage(String title, String html, String url) {
        OperationStatus status = null;
        try {
            if (Util.isEmpty(title) || Util.isEmpty(html)) return;
            Simhash simhashTitle = getNLPSimhash(title);
            if (simhashTitle == null) throw new NLPException(title);
            Document doc = Jsoup.parse(html);
            Element divContentLeft = doc.select("div[id=content_left]").first();
            //获取所有具体的搜索结果块
            Elements divResultCContainers = divContentLeft.select("div[class~=result(.+)c-container]");
            for (Element divResultCContainer : divResultCContainers) {
                //获取首个链接
                Element link = divResultCContainer.select("a[href]").first();
                //排除[百度快照]
                if (Util.isEmpty(link.text()) || link.text().startsWith(Setting.EXCLUSION_BAIDU_PREFIX)) {
                    continue;
                }
                //匹配链接
                Simhash simhashLinkText = getNLPSimhash(link.text());
                int hammimngDistance = simhashTitle.getHammingDistance(simhashLinkText);
                Logger.log(20180604.1024, "[匹配文本]" + link.text() + " (海明距离" + hammimngDistance + ")");
                if (hammimngDistance > Setting.MAX_HAMMING_DISTANCE || simhashLinkText.getTokens().size() < Setting.MIN_TOKEN_SIZE)
                    continue;
                Logger.log("[匹配链接]" + link.attr("href") + " (海明距离" + hammimngDistance + ")", true);
                //添加结果页链接至urlsDbManager
                status = urlsDbManager.putUrl(link.attr("href"), url);
                if (!OperationStatus.SUCCESS.equals(status)) {
                    Logger.log("[添加结果页失败]" + link.attr("href"), true);
                }
                status = urlsDbManager.updateWeight(link.attr("href"), CrawlUrl.DEFAULT_WEIGHT + 1);
                if (!OperationStatus.SUCCESS.equals(status)) {
                    Logger.log("[提高优先级失败]" + link.attr("href"), true);
                }
                status = urlsDbManager.putMessage(link.attr("href"), TenderParser.MESSAGE_TITLE, title);
                if (!OperationStatus.SUCCESS.equals(status)) {
                    Logger.log("[添加标题至数据库失败]" + link.attr("href"), true);
                }
            }
        } catch (NLPException e) {
            e.printStackTrace();
        }
    }

    /**
     * 爬取展示页
     *
     * @param url
     */
    protected void onCrawlingDisplayPage(String url) {
        if (Util.isEmpty(url) || !isDisplayPage(url)) return;
        OperationStatus status = null;
        try {
            //获取html文本
            String html = CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
            if (html == null) {
                Logger.log("[无法访问]" + url, true);
                urlsDbManager.updateWeightByRelativeValue(url, -1); //自降优先级
                return;
            } else {
                Logger.log("[分析展示页]" + url, true);
                urlsDbManager.updateWeightByRelativeValue(url, -1); //自降优先级,防止过程出错而循环爬取
            }
            //获取网页标题
            Document document = Jsoup.parse(html);
            String title = document.title().replace(Setting.SUFFIX_TITLE, "");
            //保存html文本至本地文件
            Path dir = Paths.get(Setting.TENDER_PARSER_DISPLAY_DIRECTORY);
            if (!Files.isDirectory(dir)) {
                Files.createDirectories(dir);
            }
            String fileName = Setting.PREFIX_DISPLAY + title + new SimpleDateFormat("_ yyyyMMdd_hhmmss").format(new Date());
            fileName = CommonAnalysis.filterFileName(fileName, "html");
            Path path = Paths.get(dir.toString(), fileName);
            if (!Files.isRegularFile(path)) {
                Files.createFile(path);
            }
            CommonAnalysis.textToFile(Util.htmlFormatting(html), dir.toString(), path.getFileName().toString());
            //上传html文本至FileServer&UserServer
            boolean resultUpload = Util.uploadFileAndGrant(path, fileServerSDK, userServerSDK, gson);
            //在搜索引擎中查找内容
            HttpUrl searchUrl = HttpUrl.parse(Setting.URL_SEARCH_ENGINE).newBuilder()
                    .addQueryParameter(Setting.QUERY_SEARCH_ENGINE, title).build();
            status = urlsDbManager.putUrl(searchUrl.toString(), url);
            if (!OperationStatus.SUCCESS.equals(status)) {
                Logger.log("[添加搜索页失败]" + searchUrl.toString(), true);
            }
            status = urlsDbManager.updateWeight(searchUrl.toString(), CrawlUrl.DEFAULT_WEIGHT + 1); //提高优先级,先爬取搜索页
            if (!OperationStatus.SUCCESS.equals(status)) {
                Logger.log("[提高优先级失败]" + searchUrl.toString(), true);
            }
            //从urlsDbManager中移除
            status = urlsDbManager.settleUrl(url, resultUpload ? Setting.STATUS_URL_SUCCESS : Setting.STATUS_URL_UPLOAD_FAILURE);
            if (!OperationStatus.SUCCESS.equals(status)) {
                Logger.log("[移除URL失败]" + url, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            onTimeout(url);
            return;
        }
    }

    /**
     * 爬取首页
     *
     * @param url
     */
    protected void onCrawlingHomePage(String url) {
        if (Util.isEmpty(url) || !isHomePage(url)) return;
        OperationStatus status;
        try {
            //获取html文本
            String html = CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
            if (html == null) {
                Logger.log("[无法访问]" + url, true);
                return;
            } else {
                Logger.log("[分析主页]" + url, true);
                OperationStatus statusUpdate = urlsDbManager.updateWeight(url, Setting.HOME_PAGE_WEIGHT); //降低优先级,先爬取展示页
                if (!OperationStatus.SUCCESS.equals(statusUpdate)) {
                    Logger.log("[优先级降低失败]" + url, true);
                    return;
                }
            }
            //保存html文本至本地文件
            Path dir = Paths.get(Setting.TENDER_PARSER_HOMEPAGE_DIRECTORY);
            if (!Files.isDirectory(dir)) {
                Files.createDirectories(dir);
            }
            String fileName = Setting.PREFIX_HOMEPAGE + new SimpleDateFormat("yyyyMMdd_hhmmss").format(new Date());
            fileName = CommonAnalysis.filterFileName(fileName, "html");
            Path path = Paths.get(dir.toString(), fileName);
            if (!Files.isRegularFile(path)) {
                Files.createFile(path);
            }
            CommonAnalysis.textToFile(Util.htmlFormatting(html), dir.toString(), path.getFileName().toString());
            //上传html文本至FileServer&UserServer
            Util.uploadFileAndGrant(path, fileServerSDK, userServerSDK, gson);
            //分析出网页中所有链接
            Document doc = Jsoup.parse(html);
            Elements links = doc.select("a[href]");
            for (Element link : links) {
                if (Util.isEmpty(link.attr("href")) || Util.isEmpty(link.text())) continue;
                //[http://www.chinazbcgou.com.cn/display.php?id=3095492]
                if (Setting.PATTERN_DISPLAY.matcher(link.attr("href")).matches()) {
                    //将链接添加至urlsDbManager
                    String displayUrl = Setting.URL_TENDER_HOMEPAGE + Constant.PATH_SEPERATOR + link.attr("href");
                    OperationStatus putStatus = urlsDbManager.putUrl(displayUrl, url, false);
                    if (OperationStatus.SUCCESS.equals(putStatus)) {
                        Logger.log("[添加展示页]" + link.text() + "(" + displayUrl + ")", true);
                        urlsDbManager.updateWeight(displayUrl, CrawlUrl.DEFAULT_WEIGHT); //设置默认优先级
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            onTimeout(url);
            return;
        }
    }

    /**
     * 通过HanLP对text进行分词,并获取经过分词的NLP
     * 仅支持中文文本!
     *
     * @param text
     * @return
     */
    protected Simhash getNLPSimhash(String text) {
        if (Util.isEmpty(text)) return null;
        List<Term> terms = HanLP.segment(text);
        List<String> tokens = terms.stream().map(term -> term.word).collect(Collectors.toList());
        Logger.log(20180604.1024, "[分词]" + tokens);
        if (tokens == null || tokens.size() == 0) return null;
        Simhash simhash = new Simhash(tokens);
        return simhash;
    }

    protected void onTimeout(String url) {
        Logger.log("[加载超时]" + url + " 重启", true);
        restart();
        OperationStatus status = urlsDbManager.updateWeightByRelativeValue(url, -1); //自降优先级
        if (!OperationStatus.SUCCESS.equals(status)) {
            Logger.log("[自降优先级]" + url, true);
        }
    }

    /**
     * 是否为搜索结果页
     *
     * @param url
     * @return
     */
    protected boolean isResultPage(String url) {
        if (Util.isEmpty(url)) return false;
        try {
            URL urlSrc = new URL(url);
            String title = urlsDbManager.getMessage(url, TenderParser.MESSAGE_TITLE);
            return !Util.isEmpty(title);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 是否为搜索引擎页
     *
     * @param url
     * @return
     */
    protected boolean isSearchPage(String url) {
        if (Util.isEmpty(url)) return false;
        try {
            URL urlSrc = new URL(url);
            URL urlSearchEngine = new URL(Setting.URL_SEARCH_ENGINE);
            return (urlSearchEngine.getHost().equals(urlSrc.getHost()) && urlSrc.getQuery().contains(Setting.QUERY_SEARCH_ENGINE + "="));
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 是否为展示页
     *
     * @param url
     * @return
     */
    protected boolean isDisplayPage(String url) {
        if (Util.isEmpty(url)) return false;
        try {
            URL urlSrc = new URL(url);
            URL urlHomePage = new URL(Setting.URL_TENDER_HOMEPAGE);
            return (urlHomePage.getHost().equals(urlSrc.getHost())
                    && (Setting.PATH_DISPLAY.equals(urlSrc.getPath())));
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 是否为首页
     *
     * @param url
     * @return
     */
    protected boolean isHomePage(String url) {
        if (Util.isEmpty(url)) return false;
        try {
            URL urlSrc = new URL(url);
            URL urlHomePage = new URL(Setting.URL_TENDER_HOMEPAGE);
            return (urlHomePage.getHost().equals(urlSrc.getHost())
                    && ("".equals(urlSrc.getPath()) || Constant.PATH_SEPERATOR.equals(urlSrc.getPath())));
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        }
    }

    protected void addHomePage(String homePageUrl) {
        if (Util.isEmpty(homePageUrl)) return;
        addUrl(homePageUrl);
        //提高优先级,先爬取主页
        urlsDbManager.updateWeight(homePageUrl, Integer.MAX_VALUE);
    }

    @Override
    protected void setup() {
        super.setup();
        fileServerSDK = new FileServerSDK();
        fileServerSDK.setLog(Setting.FLAG_FILESERVER_LOG);
        userServerSDK = new UserServerSDK();
        userServerSDK.setLog(Setting.FLAG_FILESERVER_LOG);
        userServerSDK.insert(Setting.TENDER_PARSER_USER_ID, Setting.TENDER_PARSER_USER_ACCESS_CODE);
    }
}
