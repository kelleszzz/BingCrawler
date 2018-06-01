package com.kelles.crawler.crawler.database;


import java.math.BigInteger;

import com.kelles.crawler.crawler.bean.*;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class UrlsSimHashKeyCreator  implements SecondaryKeyCreator{
	public static SerialBinding serialBinding;

	public UrlsSimHashKeyCreator(SerialBinding serialBinding) {
		super();
		this.serialBinding = serialBinding;
	}
	

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
    DatabaseEntry keyEntry, 
    DatabaseEntry dataEntry,
    DatabaseEntry resultEntry) {
		CrawlUrl crawlUrl=(CrawlUrl) serialBinding.entryToObject(dataEntry);
		if (crawlUrl.getSimHash()==null) {
			BigInteger noHash=new BigInteger("-1");
			resultEntry.setData(noHash.toByteArray());
		}
		else resultEntry.setData(crawlUrl.getSimHash().toByteArray());
		return true;
	}
}
