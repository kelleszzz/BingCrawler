package com.kelles.crawler.bingcrawler.threadpool;

import com.kelles.crawler.bingcrawler.database.weightdb.WeightInterface;

import java.io.Serializable;

public interface TaskInterface extends WeightInterface,Serializable{
	public boolean execute();
	public String describe();
	public byte[] getKeyBytes();
}
