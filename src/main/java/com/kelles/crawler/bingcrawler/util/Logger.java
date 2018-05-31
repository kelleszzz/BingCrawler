package com.kelles.crawler.bingcrawler.util;

import com.kelles.crawler.bingcrawler.setting.Setting;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class Logger {
    protected static double CURRENT_VERSION = Setting.LOG_VERSION;
    protected static boolean IF_LOG = true;
    private static FileOutputStream fos = null;

    private static boolean firstRun = true;

    private static FileOutputStream getFileStream() {
        try {
            File file = new File(Setting.LOG_PATH);
            if (firstRun && file.isFile()) {
                file.delete();
                firstRun = false;
            }
            FileOutputStream fos = new FileOutputStream(file, true);
            return fos;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    public static <T> void log(T msg) {
        log(Integer.MAX_VALUE, msg);
    }

    public static <T> void log(double version, T msg) {
        if (IF_LOG && version >= CURRENT_VERSION) {
            try {
                fos = getFileStream();
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(fos, "utf-8"));
                pw.println(msg);
                pw.flush();
                System.out.println(msg);
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                try {
                    fos.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public static boolean check(double version) {
        return version >= CURRENT_VERSION;
    }

}
