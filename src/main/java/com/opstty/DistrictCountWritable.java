package com.opstty;

import com.opstty.job.MaxTreeCountJob;
import com.opstty.job.TreeCountJob;
import org.apache.hadoop.util.ProgramDriver;

public class DistrictCountWritable {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("TreeCountByDistrict", TreeCountJob.class,
                    "A map/reduce program that counts the number of trees in each district.");

            programDriver.addClass("MaxTreeCount", MaxTreeCountJob.class,
                    "A map/reduce program that finds the district with the maximum number of trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}

