package com.kelles.crawler.crawler.bean;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import com.google.common.base.Function;

public abstract class WebDriverFunction implements Function<WebDriver,WebElement> {
	protected volatile int lastContentTextLength =0, contentSameAsLast =0;
}
