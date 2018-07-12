package communitydetection;

public class CommunityDetection {
    public static void main(String input, String output) throws Exception
    {
        Init.main(input, output + "/community_detection/iter0");
        LabelPropagation.main(output + "/community_detection");
        ResultViewer.main(output+"/community_detection/iter20", output + "/final_community");
    }
}
