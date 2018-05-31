package com.kelles.crawler.bingcrawler.parser;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.kelles.crawler.bingcrawler.setting.Setting;
import org.apache.http.HttpStatus;
import org.apache.http.util.TextUtils;
import org.openqa.selenium.NoSuchSessionException;

import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.kelles.crawler.bingcrawler.analysis.*;
import com.kelles.crawler.bingcrawler.database.*;
import com.kelles.crawler.bingcrawler.download.*;
import com.sleepycat.je.OperationStatus;


//爬取必应学术的Parser
public class BingParser extends AbstractParser {

    private DbManager<Profile> profilesManager = null;

    public static void main(String[] args) {
        BingParser parser = null;
        try {
            parser = new BingParser();
            parser.addBingAcademicTheme("artificial intelligence papers", 1); //必应学术搜索第一页
            parser.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            parser.close();
        }
    }

    @Override
    public void start() {
        menu:
        for (; ; ) {
            Logger.log(Setting.MENU + " | pf->结构化内容");
            String userInput = scanner.nextLine();
            String nextUrl = urlsDbManager.getNext();
            try {
                Logger.log("[即将分析]" + URLDecoder.decode(nextUrl, "utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            if ("pf".equals(userInput)) {
                //自定义输入
                profilesManager.describe();
            } else {
                //基础输入
                switch (handleUserInput(userInput, nextUrl)) {
                    case Setting.STATUS_USERINPUT_SUCCESS: {
                        continue menu;
                    }
                    case Setting.STATUS_USERINPUT_EXIT: {
                        return;
                    }
                }
            }
        }
    }

    @Override
    protected void setup() {
        super.setup();
        Logger.log("Initializing ProfilesManager");
        profilesManager = new DbManager<Profile>(Setting.BING_PARSER_PROFILESDB_PATH, Profile.class);
    }

    @Override
    public void close() {
        super.close();
        profilesManager.close();
    }

    /*添加bing学术上指定的主题至todoUrls*/
    public void addBingAcademicTheme(String theme, int page) {
        String prefix = "http://cn.bing.com/academic/search?q=";
        String suffix = "&sort=1";
        try {
            String url = prefix + URLEncoder.encode(theme, "utf-8") + "&first=" + ((page - 1) * 10 + 1) + suffix;
            String html = CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
            if (html == null) throw new Exception("未能获取指定Bing学术搜索页面:" + url);
            //按照自定义规则保存html文本至文件
            String fileName = CommonAnalysis.urlGetFileName(URLDecoder.decode(url, "utf-8"));
            File f = CommonAnalysis.strToFile(Util.htmlFormatting(html), Setting.HTMLURLS_PATH, fileName);
            //
            List<Profile> linkProfiles = BingAnalysis.analyzeBingAcademicSearch(html);
            if (linkProfiles != null)
                for (Profile linkProfile : linkProfiles) {
                    if (linkProfile.getUrl() != null) {
                        if (urlsDbManager.putUrl(linkProfile.getUrl()) == OperationStatus.SUCCESS) {
                            Logger.log("[添加论文链接]" + linkProfile); //
                            urlsDbManager.updateWeight(linkProfile.getUrl(), CrawlUrl.DEFAULT_WEIGHT + 10);
                        }
                    }
                }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }


    /*不是必应学术时时分析html正文*/
    protected void commonAnalyzeHtml(String html, String url) {
        //分析html
		/*Logger.log("正文内容:\n"+CommonAnalysis.getContent(html));
		ParserUtils.showHammingDistances(url); //显示和所有uniUrl的海明距离
		ParserUtils.showNodesInfo(html); //每个html结点查看
        ParserUtils.readTorrent(html); //分析种子*/
    }

    @Override
    protected void onCrawlingUrl(String url) {
        if (TextUtils.isEmpty(url)) return;
        try {
            Logger.log("[开始分析]" + URLDecoder.decode(url, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String hostUrl = Util.getHostUrl(url);
        /*判断是必应搜索还是必应Profile*/
        boolean isProfileUrl = false, isSearchUrl = false;
        isProfileUrl = BingAnalysisUtils.isBingAcademicProfileUrl(url);
        isSearchUrl = BingAnalysisUtils.isBingAcademicSearchUrl(url);
        /*获取Profile的html文本*/
        String html = null;
        if (isProfileUrl) html = BingAnalysis.seleniumGetBingAcademicProfileHtml(url, remoteDriver.getDriver());
        else html = CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
        /*添加url至uniUrls*/
        if (html == null) {
            Logger.log("[无法访问]" + url);
            urlsDbManager.settleUrl(url, HttpStatus.SC_NOT_FOUND);
            return;
        } else urlsDbManager.settleUrl(url, HttpStatus.SC_OK);
        /*按照自定义规则保存html文本至文件*/
        String fileName = null;
        try {
            fileName = CommonAnalysis.urlGetFileName(URLDecoder.decode(url, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        File f = CommonAnalysis.strToFile(Util.htmlFormatting(html), Setting.HTMLURLS_PATH, fileName);
        //TODO 上传至云端
        Util.uploadFileAndGrant(f);
        /*分析html*/
        if (isProfileUrl) {
            /*提取Profile,保存至profilesDb*/
            Profile profile = BingAnalysis.analyzeBingAcademicProfile(html);
            profilesManager.put(profile.getTitle(), profile);
            Logger.log("[读取文章]" + profile.getTitle()); //
            /*下载论文*/
            if (profile.getDownloadUrls().size() > 0) {
                DownloadTask.Builder taskBuilder = new DownloadTask.Builder();
                taskBuilder.setReferer(url);
                taskBuilder.toFile(Setting.PROFILES_PATH, Util.replaceFileBadLetter(profile.getTitle()), ".pdf");
                taskBuilder.addUrls(profile.getDownloadUrls());
                downloadPool.addTask(taskBuilder.build());
            }
            /*使用摘要生成SimHash*/
            urlsDbManager.updateSimHash(url, TextAnalysis.getSimHash(profile.getIntroduction()));
            /* 添加链接url至todoUrls*/
            List<Profile> linkProfiles = new ArrayList();
            if (profile.getReferences() != null) linkProfiles.addAll(profile.getReferences());
            if (profile.getCitedPapers() != null) linkProfiles.addAll(profile.getCitedPapers());
            for (Iterator it = linkProfiles.iterator(); it.hasNext(); ) {
                Profile linkProfile = (Profile) it.next();
                /*获取url*/
                String linkUrl = linkProfile.getUrl();
                if (linkUrl == null) continue;
                linkUrl = Util.removeSuffix(linkUrl);
                if (linkUrl.startsWith("/")) linkUrl = hostUrl + linkUrl;
                /*仅添加必应学术url*/
                if (BingAnalysisUtils.isBingAcademicSearchUrl(linkUrl)) {
                    urlsDbManager.putUrl(linkUrl, url);
                    List<String> messages = new ArrayList();
                    messages.add(linkProfile.getTitle());
                    if (linkProfile.getAuthors() != null)
                        for (String author : linkProfile.getAuthors())
                            messages.add(author);
                    urlsDbManager.updateMessages(linkUrl, messages);
                } else if (BingAnalysisUtils.isBingAcademicProfileUrl(linkUrl)) {
                    urlsDbManager.putUrl(linkUrl, url);
                    urlsDbManager.updateWeight(linkUrl, CrawlUrl.DEFAULT_WEIGHT + 1); /*提高优先级*/
                }
            }
        } else if (isSearchUrl) {
            /*提取链接*/
            List<Profile> linkProfiles = BingAnalysis.analyzeBingAcademicSearch(html);
            /*使用链接摘要生成SimHash*/
            StringBuilder content = new StringBuilder();
            for (Profile linkProfile : linkProfiles) {
                String introduction = linkProfile.getIntroduction();
                if (introduction != null) content.append(introduction + " ");
            }
            urlsDbManager.updateSimHash(url, TextAnalysis.getSimHash(content.toString()));
            Logger.log(content.toString());
            /* 添加链接url至todoUrls*/
            if (linkProfiles != null) {
                List<String> messages = urlsDbManager.getMessages(url); //获取messages
                for (Profile linkProfile : linkProfiles) {
                    if (linkProfile.getUrl() == null) continue;
                    linkProfile.setUrl(Util.removeSuffix(linkProfile.getUrl()));
                    if (linkProfile.getUrl().startsWith("/"))
                        linkProfile.setUrl(hostUrl + linkProfile.getUrl());
                    /*仅添加必应学术url*/
                    if (BingAnalysisUtils.isBingAcademicProfileUrl(linkProfile.getUrl())) {
                        if (messages != null && messages.size() > 0) {
                            /*仅添加指定搜索的文章*/
                            boolean containsProperAuthor = false, isTitleRight = false;
                            /*标题是否正确(比较前40个字符)*/
                            int compareStarts = 40;
                            String properTitle = messages.get(0);
                            String linkTitle = linkProfile.getTitle();
                            if (!properTitle.substring(0, properTitle.length() > compareStarts ? compareStarts : properTitle.length())
                                    .equalsIgnoreCase(linkTitle.substring(0, linkTitle.length() > compareStarts ? compareStarts : linkTitle.length()))) {
//            					Logger.log("[指定搜索文章]错误标题"+linkProfile.getTitle()); //
                            } else isTitleRight = true;
                            /*作者是否正确*/
                            if (messages.size() > 1) {
                                List<String> linkAuthors = linkProfile.getAuthors();
                                if (linkAuthors == null || linkAuthors.size() == 0) {
//            						Logger.log("[指定搜索文章]作者不存在"); //
                                    containsProperAuthor = false;
                                }
                                /*存在一个正确的作者即可*/
                                else for (int i = 1; i < messages.size(); i++) {
                                    if (linkAuthors.contains(messages.get(i))) {
                                        containsProperAuthor = true;
                                        break;
                                    }
                                }
                            }
                            if (isTitleRight || containsProperAuthor) {
                                /*添加指定文章,且移除深度限制*/
                                if (urlsDbManager.putUrl(linkProfile.getUrl(), url, false) == OperationStatus.SUCCESS) {
                                    Logger.log("[添加指定文章]" + linkProfile.getTitle() + "(" + linkProfile.getUrl() + ")");
                                    urlsDbManager.updateWeight(linkProfile.getUrl(), CrawlUrl.DEFAULT_WEIGHT + 1); /*提高优先级*/
                                }
                            }
                        } else {
                            Logger.log("[添加文章]" + linkProfile);
                            urlsDbManager.putUrl(linkProfile.getUrl(), url);
                            urlsDbManager.updateWeight(linkProfile.getUrl(), CrawlUrl.DEFAULT_WEIGHT + 1); /*提高优先级*/
                        }
                    } else if (BingAnalysisUtils.isBingAcademicSearchUrl(linkProfile.getUrl())) {
                        Logger.log("[添加搜索]" + linkProfile); //不太可能出现的情况
                        urlsDbManager.putUrl(linkProfile.getUrl(), url);
                    }
                }
            }
        } else {
            /*不是必应学术时,一般化分析html*/
            commonAnalyzeHtml(html, url);
            /*使用一般正文内容生成SimHash*/
            urlsDbManager.updateSimHash(url, TextAnalysis.getSimHash(CommonAnalysis.getContent(html)));
            /*添加链接url至todoUrls(暂停使用)*/
            /*Set<String> linkSet=CommonAnalysis.extractLinks(html,Util.getHostUrl(url)); //
            for (Iterator it=linkSet.iterator();it.hasNext();){
            	String linkUrl=(String) it.next();
            	linkUrl=Util.removeSuffix(linkUrl);
            	manager.putUrl(linkUrl,url);
            }*/
        }
    }
}
