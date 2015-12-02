package crawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

import java.util.Set;

public class LargeScaleCrawler extends WebCrawler {

	public static String outputLines = "";

	@Override
	public void visit(Page page) {

		String url = page.getWebURL().getURL();

		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			String text = htmlParseData.getText();
			String html = htmlParseData.getHtml();
			Set<WebURL> links = htmlParseData.getOutgoingUrls();
			Boolean first = true;

			outputLines += url;

			// rank set to 1
			outputLines += "\t1\t";
			// // nb of outgoing link
			// outputLines += " " + links.size() + " ";
			for (WebURL webURL : links) {
				if (first) {
					first = false;
					// each outgoing link for url page key
					outputLines += webURL.getURL();
				} else {
					// each outgoing link for url page key
					outputLines += "," + webURL.getURL();
				}

			}
			outputLines += "\n";
		}
	}
}
