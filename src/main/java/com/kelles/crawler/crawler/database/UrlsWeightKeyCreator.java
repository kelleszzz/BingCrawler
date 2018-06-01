package com.kelles.crawler.crawler.database;

import com.kelles.crawler.crawler.util.*;
import com.kelles.crawler.crawler.bean.*;
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
		resultEntry.setData(Util.intToByteArray(crawlUrl.getWeight()));
		return true;
	}
}
