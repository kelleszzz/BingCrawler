package com.kelles.crawler.crawler.util;

import com.google.gson.Gson;
import com.kelles.crawler.crawler.setting.Setting;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Logger {
    protected static double CURRENT_VERSION = Setting.LOG_VERSION;
    protected static boolean IF_LOG = true;
    private static FileOutputStream fos = null;
    private static Gson gson = new Gson();

    private static boolean firstRun = true;

    private static FileOutputStream getFileStream() {
        try {
            File dir = new File(Setting.ROOT);
            if (!dir.isDirectory()) {
                dir.mkdirs();
            }
            File file = new File(Setting.LOG_PATH);
            if (firstRun && file.isFile()) {
                file.delete();
                firstRun = false;
            }
            if (!file.isFile()) {
                file.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(file, true);
            return fos;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T> void log(T obj) {
        log(Double.MAX_VALUE, obj, true);
    }

    public static <T> void log(T obj, boolean decode) {
        log(Double.MAX_VALUE, obj, decode);
    }

    public static <T> void log(double version, T obj) {
        log(version, obj, false);
    }

    public static <T> void log(double version, T obj, boolean decode) {
        if (IF_LOG && check(version)) {
            String msg = obj instanceof String ? (String) obj : gson.toJson(obj);
            try {
                if (decode) {
                    msg = URLDecoder.decode(msg, Setting.DEFAULT_CHARSET.displayName());
                }
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
