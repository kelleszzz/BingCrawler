package com.kelles.crawler.crawler.setting;

import com.kelles.crawler.crawler.bean.CrawlUrl;
import org.apache.http.HttpStatus;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

public class Setting {
    //基础文件配置
    public static final String CHROME_DRIVER_PATH = "lib/chromedriver.exe";
    public static final String CHROME_USER_DATA = "lib/ChromeUserData";
    public static final String ANSJ_LIBRARY = "lib/ansj_library/default.dic";
    public static final String ROOT = "CrawledData";
    public static final String DATA_ANALYSIS = "/DataAnalysis";
    public static final String LOG_PATH = ROOT + "/Console.log";
    public final static String BING_PARSER_URLSDB_PATH = Setting.ROOT + "/bing_urlsdb_home"; //Urls数据库文件夹路径
    public final static String BING_PARSER_PROFILESDB_PATH = Setting.ROOT + "/bing_profilesdb_home"; //Urls数据库文件夹路径
    public final static String BING_PARSER_DOWNLOADPOOL_PATH = Setting.ROOT + "/bing_downloadpool_home"; //下载线程池数据库地址
    public static final int TIMEOUT_LOAD_HTML = 3000;
    public static final int TIMEOUT_NAVIGATOR = 15000;
    public static final int TIME_POLLING_EVERY_LOAD_HTML = 500;
    public static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

    //Parser配置
    public static final Double LOG_VERSION = 20180604.0125;
    public final static int STATUS_USERINPUT_EXIT = -1;
    public final static int STATUS_USERINPUT_SUCCESS = 1;
    public final static int STATUS_URL_SUCCESS = 1;
    public final static int STATUS_URL_NOT_FOUND = -1;
    public final static int STATUS_URL_PASS = -2;
    public final static int STATUS_URL_UPLOAD_FAILURE = -3;
    public final static int MAX_FILE_NAME_LENGTH = 150;
    public final static String PREFIX_FILE_CHARCODE = "_", SUFFIX_FILE_CHARCODE = "";

    //Menu
    public static final String MENU_DB = "[菜单]1->urls | 2->todoUrls | 3->uniUrls | 4->downloads | 5->清除downloads | 其它字符->返回";
    public static final String MENU = "[菜单]任意字符->继续 | p->跳过 | db->查看数据库 | auto->自动爬取 | download->仅开始下载 | restart->重启 | exit->退出";

    //BingParser
    public final static String PROFILES_PATH = Setting.ROOT + "/Profiles"; //下载论文文件夹
    public final static String HTMLURLS_PATH = Setting.ROOT + "/html_urls"; //下载html源代码文件夹

    //TenderParser
    public static final String TENDER_PARSER_USER_ID = "TenderParser";
    public static final String TENDER_PARSER_USER_ACCESS_CODE = "tom44123";
    public final static String URL_TENDER_HOMEPAGE = "http://www.chinazbcgou.com.cn";
    public final static String TENDER_PARSER_DATA_PATH = Setting.ROOT + "/tender_pasrser"; //下载招投标信息根目录
    public final static String TENDER_PARSER_HOMEPAGE_DIRECTORY = Setting.TENDER_PARSER_DATA_PATH + "/homepage"; //主页html文件
    public final static String TENDER_PARSER_DISPLAY_DIRECTORY = Setting.TENDER_PARSER_DATA_PATH + "/display"; //展示页html文件
    public final static String TENDER_PARSER_RESULT_DIRECTORY = Setting.TENDER_PARSER_DATA_PATH + "/result"; //结果页html文件
    public final static String PREFIX_HOMEPAGE = "主页_";
    public final static String PREFIX_DISPLAY = "展示页_";
    public final static String PREFIX_RESULT = "结果页_";
    public final static String PREFIX_DATA = "数据_";
    public final static String SUFFIX_TITLE = "_中国招投标采购网 官网";
    public final static String PATH_DISPLAY = "/display.php";
    public final static Pattern PATTERN_DISPLAY = Pattern.compile("display.php\\?id=(.*)");
    public static final boolean FLAG_FILESERVER_LOG = false;
    public final static String URL_SEARCH_ENGINE = "https://www.baidu.com/baidu";
    public final static String QUERY_SEARCH_ENGINE = "wd";
    public final static String EXCLUSION_BAIDU_PREFIX = "百度";
    public final static int MAX_HAMMING_DISTANCE = 60; //链接匹配最大海明距离
    public final static int MIN_TOKEN_SIZE = 3; //链接匹配最小分词数
    public final static int HOME_PAGE_WEIGHT = CrawlUrl.DEFAULT_WEIGHT - 30;
}
