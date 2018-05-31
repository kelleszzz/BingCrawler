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

    public static final String MENU_DB="[菜单]1->urls | 2->downloads | 3->清除downloads | 其它字符->返回";
    public static final String MENU="[菜单]任意字符->继续 | p->跳过 | db->查看数据库 | auto->自动爬取 | download->仅开始下载 | exit->退出";

    public static final Double LOG_VERSION=20180531.09;
    public final static int STATUS_USERINPUT_EXIT = -1;
    public final static int STATUS_USERINPUT_SUCCESS = 1;
}
