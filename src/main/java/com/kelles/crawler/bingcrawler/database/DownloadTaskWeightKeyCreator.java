package com.kelles.crawler.bingcrawler.database;

import com.kelles.crawler.bingcrawler.download.DownloadTask;
import com.kelles.crawler.bingcrawler.util.*;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class DownloadTaskWeightKeyCreator  implements SecondaryKeyCreator{
	public static SerialBinding serialBinding;

	public DownloadTaskWeightKeyCreator(SerialBinding serialBinding) {
		super();
		this.serialBinding = serialBinding;
	}
	

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
    DatabaseEntry keyEntry, 
    DatabaseEntry dataEntry,
    DatabaseEntry resultEntry) {
		DownloadTask task=(DownloadTask) serialBinding.entryToObject(dataEntry);
		resultEntry.setData(Util.intToByteArray(task.getWeight()));
		return true;
	}
}
