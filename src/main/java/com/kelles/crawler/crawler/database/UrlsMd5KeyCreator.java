package com.kelles.crawler.crawler.database;
import java.io.UnsupportedEncodingException;

import com.kelles.crawler.crawler.util.*;
import com.kelles.crawler.crawler.bean.*;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class UrlsMd5KeyCreator implements SecondaryKeyCreator{
	
	
	
	public static SerialBinding serialBinding;
	
	public UrlsMd5KeyCreator(SerialBinding serialBinding){
		this.serialBinding=serialBinding;
	}

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
    DatabaseEntry keyEntry, 
    DatabaseEntry dataEntry,
    DatabaseEntry resultEntry) {
		CrawlUrl crawlUrl=(CrawlUrl) serialBinding.entryToObject(dataEntry);
		if (crawlUrl.getMd5()==null) crawlUrl.setMd5(Md5.get(crawlUrl.getUrl()));
		try {
			resultEntry.setData(crawlUrl.getMd5().getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
