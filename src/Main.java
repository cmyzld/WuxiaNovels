import communitydetection.CommunityDetection;
import pagerank.PageRank;
import parser.JobParser;

public class Main {
    //[Input directory] [outputDirectory] [peopleList]
    public static void main(String[] args) throws Exception{
        JobParser.main(args[0], args[1] + "/iter0", args[2]);
        PageRank.main(args[1]);
        CommunityDetection.main(args[1]+"/iter0", args[1]);
    }
}
