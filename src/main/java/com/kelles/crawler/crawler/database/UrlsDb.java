package com.kelles.crawler.crawler.database;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.kelles.crawler.crawler.util.*;
import com.kelles.crawler.crawler.bean.*;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.TransactionConfig;

public class UrlsDb {
    private String homePath = null; //数据库文件夹
    public static String TODOURLS_PATH = "todourls_db";
    public static String UNIURLS_PATH = "uniurls_db";
    public static String CLASSCATALOGDB_PATH = "classcatalog_db";
    public static String TODOURLS_MD5SECDB_PATH = "todourlsmd5secdb";
    public static String TODOURLS_WEIGHTSECDB_PATH = "todourlsweightsecdb";
    public static String TODOURLS_SIMHASHSECDB_PATH = "todourlssimhashsecdb";
    public static String UNIURLS_MD5SECDB_PATH = "uniurlsmd5secdb";
    protected static boolean readOnly = false;

    protected Environment env;
    protected EnvironmentConfig envConf = new EnvironmentConfig();
    protected DatabaseConfig dbConf = new DatabaseConfig();
    ;
    protected Database todoUrls;
    protected Database uniUrls;
    protected Database classCatalogDb;
    protected StoredClassCatalog classCatalog;
    protected SerialBinding serialBinding;
    protected TransactionConfig txnConf; //事务设置,通过事务同步至disk

    //simHash的SecondaryDatabase
    protected SecondaryDatabase uniUrlsBySimHash;

    /*weight的SecondaryDatabase*/
    protected SecondaryDatabase todoUrlsByWeight;

    //md5的SecondaryDatabase
    protected SecondaryDatabase todoUrlsByMd5;
    protected SecondaryDatabase uniUrlsByMd5;

    //用于close函数
    private Set<Database> databasesToClose = new HashSet();

    @Override
    protected void finalize() throws Throwable {
        // TODO Auto-generated method stub
        super.finalize();
        close();
    }

    protected void setup() {
        File file = new File(homePath);
        if (!file.isDirectory()) file.mkdirs();

        envConf.setAllowCreate(!readOnly);
        envConf.setReadOnly(readOnly);
        envConf.setTransactional(true); //事务
        env = new Environment(file, envConf);

        dbConf.setAllowCreate(!readOnly);
        dbConf.setReadOnly(readOnly);
        dbConf.setTransactional(true); //事务

        todoUrls = env.openDatabase(null, TODOURLS_PATH, dbConf);
        uniUrls = env.openDatabase(null, UNIURLS_PATH, dbConf);

        classCatalogDb = env.openDatabase(null, CLASSCATALOGDB_PATH, dbConf);
        classCatalog = new StoredClassCatalog(classCatalogDb);
        serialBinding = new SerialBinding(classCatalog, CrawlUrl.class);

        /*事务设置*/
        Durability tranDura =
                new Durability(Durability.SyncPolicy.SYNC,
                        null,    // unused by non-HA applications.
                        null);   // unused by non-HA applications.
        txnConf = new TransactionConfig();
        txnConf.setDurability(tranDura);

        databasesToClose.add(todoUrls);
        databasesToClose.add(uniUrls);
        databasesToClose.add(classCatalogDb);

        //创建simHash的secondaryKey
        UrlsSimHashKeyCreator simHashKeyCreator = new UrlsSimHashKeyCreator(serialBinding);
        SecondaryConfig simHashSecConf = getSecConf(simHashKeyCreator);
        uniUrlsBySimHash = env.openSecondaryDatabase(null, TODOURLS_SIMHASHSECDB_PATH, uniUrls, simHashSecConf);
        databasesToClose.add(uniUrlsBySimHash);

        //创建weight的secondaryKey
        UrlsWeightKeyCreator weightKeyCreator = new UrlsWeightKeyCreator(serialBinding);
        SecondaryConfig weightSecConf = getSecConf(weightKeyCreator);
        todoUrlsByWeight = env.openSecondaryDatabase(null, TODOURLS_WEIGHTSECDB_PATH, todoUrls, weightSecConf);
        databasesToClose.add(todoUrlsByWeight);

        //创建md5的secondaryKey
        UrlsMd5KeyCreator md5KeyCreator = new UrlsMd5KeyCreator(serialBinding);
        SecondaryConfig md5SecConf = getSecConf(md5KeyCreator);
        todoUrlsByMd5 = env.openSecondaryDatabase(null, TODOURLS_MD5SECDB_PATH, todoUrls, md5SecConf);
        uniUrlsByMd5 = env.openSecondaryDatabase(null, UNIURLS_MD5SECDB_PATH, uniUrls, md5SecConf);
        databasesToClose.add(todoUrlsByMd5);
        databasesToClose.add(uniUrlsByMd5);
    }

    protected SecondaryConfig getSecConf(SecondaryKeyCreator secKeyCreator) {
        SecondaryConfig secConf = new SecondaryConfig();
        secConf.setTransactional(true); //事务
        secConf.setAllowPopulate(true);
        secConf.setAllowCreate(!readOnly);
        secConf.setReadOnly(readOnly);
        secConf.setKeyCreator(secKeyCreator);
        secConf.setSortedDuplicates(true);
        return secConf;
    }

    public void close() {
        try {
            Set<Database> secDbs = new HashSet();
            //先关闭SecondaryDatabase
            for (Database databaseToClose : databasesToClose)
                if (databaseToClose instanceof SecondaryDatabase) {
                    databaseToClose.close();
                    secDbs.add(databaseToClose);
                }
            databasesToClose.removeAll(secDbs);
            //再关闭PrimaryDatabase
            for (Database databaseToClose : databasesToClose) databaseToClose.close();
            databasesToClose.clear();
            //关闭环境
            env.close();
        } catch (Exception e) {
        }
    }

    public void describe(boolean describeTodo,boolean descrebeUni) {
        if (Logger.check(1)) {
            Logger.log("数据库路径: " + env.getHome().getAbsolutePath());
            List<String> dbNames = env.getDatabaseNames();
            Logger.log("数据库名: " + dbNames);
        }
        if (describeTodo) {
            describeDb(todoUrls);
        }
        if (descrebeUni) {
            describeDb(uniUrls);
        }
    }

    public void describeUniUrls() {
        if (Logger.check(1)) {
            Logger.log("数据库路径: " + env.getHome().getAbsolutePath());
            List<String> dbNames = env.getDatabaseNames();
            Logger.log("数据库名: " + dbNames);
        }
        describeDb(uniUrls);
    }

    private void describeDb(Database db) {
        Cursor cursor = null;
        try {
            Logger.log("数据库" + db.getDatabaseName() + "包含Url:");
            cursor = db.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            int i = 0;
            while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                try {
                    Logger.log("[" + (++i) + "]" + "key = " + new String(key.getData(), "utf-8"));
                } catch (UnsupportedEncodingException e) {
                }
                Logger.log("value = " + serialBinding.entryToObject(value));
            }
            ;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cursor != null) cursor.close();
        }
    }


    public UrlsDb(String homeDirPath) {
        super();
        this.homePath = homeDirPath;
        setup();
    }

}