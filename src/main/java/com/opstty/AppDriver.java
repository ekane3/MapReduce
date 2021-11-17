package com.opstty;

import com.opstty.job.*;
import org.apache.commons.math3.analysis.function.Max;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,"A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district", District.class ,"A map/reduce program that displays the list of distinct containing trees in this file.");
            programDriver.addClass("species", Species.class ,"A map/reduce program that displays  the list of different species trees in this file.");
            programDriver.addClass("treeskind", Treeskind.class,"A map/reduce program that calculates the number of trees of each kinds");
            programDriver.addClass("maxheightkind", Maxheightkind.class,"A map/reduce program that  calculates the height of the tallest tree of each kind.");
            programDriver.addClass("sortTrees", SortTrees.class,"A map/reduce program that sort the trees height from smallest to largest.");
            programDriver.addClass("oldestTreeDistrict", OldestTreeDistrict.class,"A map/reduce program that returns the district(s) with the oldest tree(s) in the Remarkable Trees of Paris dataset, using a sort.");
            programDriver.addClass("districtmaxtrees", DistrictMaxTrees.class,"A map/reduce program that returns the district(s) with the most trees in the Remarkable Trees of Paris dataset, checking through all the data, using the Reducer's cleanup.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
