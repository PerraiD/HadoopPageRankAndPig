import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class PageRankMapReduce {

	public static class PageRankMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int pageTabIndex = value.find("\t");
			int rankTabIndex = value.find("\t", pageTabIndex + 1);

            System.out.println(pageTabIndex);
            //dans le cas ou il y'aurai une ligne vide dans le fichier
            if(pageTabIndex == -1){
               return;
            }

			// on récupère la page extraite
			String page = Text.decode(value.getBytes(), 0, pageTabIndex);

			// la page + le page rank
			String pageWithRank = Text.decode(value.getBytes(), 0,
					rankTabIndex + 1);

			// on marque la page comme existante
			context.write(new Text(page), new Text("!"));

			// on récupère la liste des liens
			String links = Text.decode(value.getBytes(), rankTabIndex + 1,
					value.getLength() - (rankTabIndex + 1));

			// on récupère chaque page en découpant la ligne par virgule
			String[] allOtherPages = links.split(",");

			// on compte le nombre de lien sortant
			int totalLinks = allOtherPages.length;

			for (String otherPage : allOtherPages) {
				Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
				context.write(new Text(otherPage), pageRankTotalLinks);
			}

			// on récupère les liens pour la sortie
			context.write(new Text(page), new Text("|" + links));
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

		private static final float randomSurfer = 0.85F;

		@Override
		public void reduce(Text page, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			boolean isExistingPage = false;
			String[] split;
			float participantRank = 0;
			String links = "";
			String pageWithRank;

			for (Text value : values) {
				pageWithRank = value.toString();

				if (pageWithRank.equals("!")) {
					isExistingPage = true;
					continue;
				}

				if (pageWithRank.startsWith("|")) {
					links = "\t" + pageWithRank.substring(1);
					continue;
				}

				split = pageWithRank.split("\t");

				float pageRank = Float.valueOf(split[1]);
				int nbOutLink = Integer.valueOf(split[2]);

				participantRank += (pageRank / nbOutLink);
			}

			if (!isExistingPage) {
				return;
			}
			float newRank = (randomSurfer * participantRank) + (1 - randomSurfer);

			context.write(page, new Text(newRank + links));
		}
	}

	public static void main(String[] args) throws Exception {
		// args[0] => le lien vers le fichier d'input originale
		// args[1] => nbItération

		Configuration conf = new Configuration();

		Job pageRankCalculation = Job.getInstance(conf, "pageRank");
		pageRankCalculation.setJarByClass(PageRankMapReduce.class);

		pageRankCalculation.setOutputKeyClass(Text.class);
		pageRankCalculation.setOutputValueClass(Text.class);

		// on retire les fichiers de la dernière expérience
		File file = new File(System.getProperty("user.dir")
				+ "../../PageRankResults");
		FileUtils.deleteDirectory(file);

		FileInputFormat.setInputPaths(pageRankCalculation, new Path(args[0]));
		FileOutputFormat.setOutputPath(pageRankCalculation, new Path(
				"../../PageRankResults/turn1"));

		pageRankCalculation.setMapperClass(PageRankMapper.class);
		pageRankCalculation.setReducerClass(PageRankReducer.class);

		// on attend la complétion du processus
		pageRankCalculation.waitForCompletion(true);
		// itération sur les résultats précédent
		for (int i = 2; i <= new Integer(args[1]); i++) {
			conf = new Configuration();

			pageRankCalculation = Job.getInstance(conf, "pageRank");
			pageRankCalculation.setJarByClass(PageRankMapReduce.class);

			pageRankCalculation.setOutputKeyClass(Text.class);
			pageRankCalculation.setOutputValueClass(Text.class);

			// // on retire les fichiers de la dernière expérience
			// File file = new File("");

			FileInputFormat.setInputPaths(pageRankCalculation, new Path(
					"../PageRankResults/turn" + (i - 1) + "/part-r-00000"));
			FileOutputFormat.setOutputPath(pageRankCalculation, new Path(
					"../PageRankResults/turn" + i));

			pageRankCalculation.setMapperClass(PageRankMapper.class);
			pageRankCalculation.setReducerClass(PageRankReducer.class);
			// on attend la complétion du processuss
			pageRankCalculation.waitForCompletion(true);
		}

	}
}