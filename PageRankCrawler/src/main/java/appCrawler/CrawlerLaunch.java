package appCrawler;

import crawler.LargeScaleCrawler;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class CrawlerLaunch {

	/**
	 * @param args
	 */
	public static void main(String[] args) {


		String crawlStorageFolder = System.getProperty("user.dir") + "/_temp/";
		int numberOfCrawlers = 7;

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(crawlStorageFolder);

		config.setMaxDepthOfCrawling(Integer.valueOf(args[1]));

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig,
				pageFetcher);
		CrawlController controller;
		try {
			controller = new CrawlController(config, pageFetcher,
					robotstxtServer);


			controller.addSeed(args[0]);

			/*
			 * Start the crawl. This is a blocking operation, meaning that your
			 * code will reach the line after this only when crawling is
			 * finished.
			 */
            System.out.println("Crawler is running");

            controller.start(LargeScaleCrawler.class, numberOfCrawlers);


            File file = new File(System.getProperty("user.dir")
					+ "/output/PageRankInput.txt");

            // if file doesnt exists, then create it
			if (file.exists()) {
				file.delete();
			}

			file.createNewFile();

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(LargeScaleCrawler.outputLines);
			bw.close();
			System.out.println("Task done");
		} catch (Exception e) {

			e.printStackTrace();
		}
	}
}
