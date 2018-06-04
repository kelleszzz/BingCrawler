package com.kelles.crawler.crawler.setting;

public class Constant {
    public static final String Load_More = "Load More";
    public static final String Expand = "Expand";
    public static final String Source = "Source";
    public static final String Download = "Download";
    public static final String References = "References";
    public static final String Cited_Papers = "Cited Papers";
    public static final String Authors = "Authors";
    public static final String Introduction = "Introduction";
    public static final String Keywords = "Keywords";
    public static final String Year = "Year";
    public static final String Journal = "Journal";
    public static final String Volume = "Volume";
    public static final String Issue = "Issue";
    public static final String Pages = "Pages";
    public static final String Cited_By = "Cited by";
    public static final String DOI = "DOI";
    public static final String PATH_SEPERATOR = "/";

    public static final String REDIRECT_HTML = "<HTML>  \n" +
            "<HEAD>  \n" +
            "<meta content=\"text/html; charset=utf-8\" http-equiv=\"content-type\">\n" +
            "<TITLE> Redirect Page </TITLE>  \n" +
            "<meta http-equiv=\"refresh\" content=\"0; URL=YOURURLHERE\">  \n" +
            "</HEAD>  \n" +
            "  \n" +
            "<BODY>  \n" +
            "稍等,正在重定向到论文页面~❤  by Kelles\n" +
            "</BODY>  \n" +
            "</HTML>  ";
}
