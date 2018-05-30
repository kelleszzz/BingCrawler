package com.kelles.crawler.bingcrawler.bean;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import com.google.common.base.Function;

public abstract class WebDriverFunction implements Function<WebDriver,WebElement> {
	protected volatile int arg0=0,arg1=0;
}
