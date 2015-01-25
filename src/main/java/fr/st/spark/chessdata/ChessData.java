package fr.st.spark.chessdata;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaSparkContext;

class ChessData {

    public static void main(String[] arg) {
        JavaSparkContext sc = new JavaSparkContext("local[16]", "chess");

        StopWatch sw = new StopWatch();
        sw.start();

        int[] count = sc.textFile(PGNPath.PGN_PATH)
                .filter(line -> line.startsWith("[Result"))
                .map(ChessData::mapToArrayResult)
                .reduce(ChessData::sum);

        sw.stop();

        System.out.println(count[0] + " " + count[1] + " " + count[2]);
        System.out.println("Duration : " + sw.toString());
    }

    private static int[] mapToArrayResult(String value) {
        int[] array;

        char c = value.charAt(value.length() - 3);  //decode black value

        if (c == '1') {
            array = new int[]{0, 1, 0};
        } else if (c == '0') {
            array = new int[]{1, 0, 0};
        } else if (c == '2') {
            array = new int[]{0, 0, 1};
        } else {
            array = new int[]{0, 0, 0};
        }

        return array;
    }

    private static int[] sum(int[] a, int[] b) {
        int[] c = new int[3];

        c[0] = a[0] + b[0];
        c[1] = a[1] + b[1];
        c[2] = a[2] + b[2];

        return c;
    }

}
