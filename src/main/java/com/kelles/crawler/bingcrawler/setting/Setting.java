package com.kelles.crawler.bingcrawler.setting;

public class Setting {
    public static final String CHROME_DRIVER_PATH ="lib/chromedriver.exe";
    public static final String CHROME_USER_DATA ="lib/ChromeUserData";
    public static final String ANSJ_LIBRARY ="lib/ansj_library/default.dic";

    public static final String ROOT="CrawledData/";
    public static final String DATA_ANALYSIS="DataAnalysis/";
    public static final String LOG_PATH=ROOT+"Console.log";

    public final static String BING_PARSER_URLSDB_PATH= Setting.ROOT+"bing_urlsdb_home"; //Urls数据库文件夹路径
    public final static String BING_PARSER_PROFILESDB_PATH= Setting.ROOT+"bing_profilesdb_home"; //Urls数据库文件夹路径
    public final static String BING_PARSER_DOWNLOADPOOL_PATH= Setting.ROOT+"bing_downloadpool_home"; //下载线程池数据库地址
    public final static String PROFILES_PATH= Setting.ROOT+"Profiles"; //下载论文文件夹
    public final static String HTMLURLS_PATH= Setting.ROOT+"html_urls"; //下载html源代码文件夹

    public static final String USER_ID="BingCrawler";
    public static final String USER_ACCESS_CODE="tom44123";
}
