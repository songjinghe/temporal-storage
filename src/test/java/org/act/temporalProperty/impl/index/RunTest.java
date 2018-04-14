package org.act.temporalProperty.impl.index;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Thread.sleep;

public class RunTest {

    String filePathSrc = "writeResultSrc.txt";
    String filePathDes = "writeResultDes.txt";

    @Test
    public void main() throws Throwable {

        File fileOutputSrc = new File(filePathSrc);
        File fileOutputDes = new File(filePathDes);
        PrintWriter outSrc = new PrintWriter(new BufferedWriter(new FileWriter(fileOutputSrc)));
        PrintWriter outDes = new PrintWriter(new BufferedWriter(new FileWriter(fileOutputDes)));
        List<File> dataDir = getDataFiles();

        //dataDir.size()
        for (int fileCount = 3; fileCount <= 5; fileCount++) {

            long writeCount = 0;
            long writeTime = 0;
            for (int num = 0; num < 3; num++) {

                PerformanceTest writeTest = new PerformanceTest(fileCount);
                writeTest.writeTest();
                writeCount = writeTest.getWriteCount();
                long writeTimeTmp = writeTest.getWriteTime();
                outSrc.println(fileCount + "   " + writeCount + "  " + writeTimeTmp);

                writeTime += writeTimeTmp;
            }

            writeTime /= 3;
            outDes.println(fileCount + "    " + writeCount + "  " + writeTime);
        }

        outSrc.close();
        outDes.close();
    }

    private List<File> getDataFiles() {
        String dataPath;
        if(SystemUtils.IS_OS_WINDOWS){
            dataPath = "C:\\Users\\Administrator\\Desktop\\TGraph-source\\20101104.tar\\20101104";
        }else{
            dataPath = "/home/song/tmp/road data/20101104";
        }

        File dataDir = new File(dataPath);
        return Arrays.asList(dataDir.listFiles());
    }


}
