package com.kelles.crawler.bingcrawler.database;

import java.io.UnsupportedEncodingException;

import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class UrlsWeightKeyCreator  implements SecondaryKeyCreator{
	public static SerialBinding serialBinding;

	public UrlsWeightKeyCreator(SerialBinding serialBinding) {
		super();
		this.serialBinding = serialBinding;
	}
	

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
    DatabaseEntry keyEntry, 
    DatabaseEntry dataEntry,
    DatabaseEntry resultEntry) {
		CrawlUrl crawlUrl=(CrawlUrl) serialBinding.entryToObject(dataEntry);
		resultEntry.setData(Utils.intToByteArray(crawlUrl.getWeight()));
		return true;
	}
}
