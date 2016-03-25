package org.embulk.filter.crawler;

import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.BinaryParseData;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.parser.ParseData;
import edu.uci.ics.crawler4j.parser.TextParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class EmbulkCrawler extends WebCrawler
{

    CrawlStat myCrawlStat;
    Pattern IMAGE_EXTENSIONS = null;

    public EmbulkCrawler()
    {
      myCrawlStat = new CrawlStat();
    }

    private Map<String, Object> params;

    @SuppressWarnings("unchecked")
    @Override
    public void onStart()
    {
        params = (Map<String, Object>) myController.getCustomData();
        Object regex = params.get("should_not_visit_pattern");
        if (regex != null) {
            IMAGE_EXTENSIONS = Pattern.compile((String)regex);
        }
    }

    /**
     * You should implement this function to specify whether the given url
     * should be crawled or not (based on your crawling logic).
     */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url)
    {
        String href = url.getURL().toLowerCase();
        if (IMAGE_EXTENSIONS != null) {
            if (IMAGE_EXTENSIONS.matcher(href).matches()) {
                return false;
            }
        }

        return url.getDomain().equals(referringPage.getWebURL().getDomain());
    }

    /**
     * This function is called when a page is fetched and ready to be processed
     * by your program.
     */
    @Override
    public void visit(Page page)
    {
        final WebURL webURL = page.getWebURL();
        final String outputPrefix = (String) params.get("output_prefix");

        Map<String, Object> map = Maps.newHashMap();
        map.put(outputPrefix + Constants.URL, webURL.getURL());
        map.put(outputPrefix + Constants.DOMAIN, webURL.getDomain());
        map.put(outputPrefix + Constants.SUBDOMAIN, webURL.getSubDomain());
        map.put(outputPrefix + Constants.PATH, webURL.getPath());
        map.put(outputPrefix + Constants.ANCHOR, webURL.getAnchor());
        map.put(outputPrefix + Constants.PARENT_URL, webURL.getParentUrl());
        map.put(outputPrefix + Constants.CONTENT_CHARSET, page.getContentCharset());
        map.put(outputPrefix + Constants.REDIRECT_TO_URL, page.getRedirectedToUrl());
        map.put(outputPrefix + Constants.LANGUAGE, page.getLanguage());
        map.put(outputPrefix + Constants.STATUS_CODE, page.getStatusCode());

        ParseData parseData = page.getParseData();
        if (parseData instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) parseData;
            map.put(outputPrefix + Constants.TITLE, htmlParseData.getTitle());
            map.put(outputPrefix + Constants.TEXT, htmlParseData.getText());
            map.put(outputPrefix + Constants.HTML, htmlParseData.getHtml());
        }
        else if (parseData instanceof TextParseData) {
            TextParseData textParseData = (TextParseData) parseData;
            map.put(outputPrefix + Constants.TEXT, textParseData.getTextContent());
        }
        else if (parseData instanceof BinaryParseData) {
            BinaryParseData binaryParseData = (BinaryParseData) parseData;
            map.put(outputPrefix + Constants.HTML, binaryParseData.getHtml());
        }
        logger.info("{}", webURL.getURL());
        myCrawlStat.pages.add(map);
    }

    @Override
    public Object getMyLocalData()
    {
      return myCrawlStat;
    }
}
