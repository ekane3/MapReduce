package com.opstty;

import com.opstty.job.District;
import com.opstty.job.Species;
import com.opstty.job.Treeskind;
import com.opstty.job.WordCount;
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
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
