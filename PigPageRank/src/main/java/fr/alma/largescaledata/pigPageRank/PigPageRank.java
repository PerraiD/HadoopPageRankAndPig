package fr.alma.largescaledata.pigPageRank;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.io.IOException;

public class PigPageRank
{
    public static void main( String[] args )
    {


        try {

            PigServer pigServer = new PigServer(ExecType.LOCAL);

            // we parse each line to provide a type to each row elements
//            pigServer.registerQuery("currentPageRank =" +
//                    "  LOAD '"+args[0]+"' " +
//                    "  USING PigStorage('\\t')" +
//                    "  AS ( url: chararray, pagerank: float, links: chararray );");


            pigServer.registerQuery("currentPageRank =" +
                    "  LOAD '"+args[0]+"' " +
                    "  USING PigStorage('\\t')" +
                    "  AS ( url: chararray, pagerank: float, links:{ link: ( url: chararray ) });");

            pigServer.dumpSchema("currentPageRank");

//            //we split the links to be an array of links and be countable
//            pigServer.registerQuery("currentPageRank  = "+
//                    "FOREACH currentPageRank  " +
//                    "GENERATE url, pagerank, TOKENIZE(links,',') AS links:{link:tuple(url:chararray)} ;"
//
//
//            );

            pigServer.dumpSchema("currentPageRank");

            //we calculate the page rank with the output link page rank
            pigServer.registerQuery("outlinkPageRank = " +
                    "FOREACH currentPageRank  " +
                    "GENERATE " +
                    "pagerank / COUNT ( links ) AS pagerank," +
                    "FLATTEN ( links ) AS to_url;");



            // we group by url and recalculate the page rank of the link with the sum of outlink page rank
            pigServer.registerQuery(" newPageRank =" +
                    "FOREACH " +
                    "( COGROUP outlinkPageRank BY to_url, currentPageRank BY url INNER )" +
                    "GENERATE " +
                    "group AS url, " +
                    "( 1.0 - 0.85 ) + 0.85 * SUM ( outlinkPageRank.pagerank ) AS pagerank, " +
                    "FLATTEN ( currentPageRank.links ) AS links;");

//            pigServer.registerQuery(" newPageRank =" +
//                    "FOREACH  newPageRank " +
//                    "GENERATE url, pagerank, FLATTEN (links) as links;");
//
//            pigServer.registerQuery(" newPageRank =" +
//                    "FOREACH  newPageRank " +
//                    "GENERATE url, pagerank, FLATTEN (links) as links;");


            pigServer.dumpSchema("newPageRank");


            //we get the new calculate page rank in a outputfile (named  part-m-00000)
            pigServer.store("currentPageRank", System.getProperty("user.dir")+"/output/pagerank3");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
