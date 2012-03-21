package com.thinkaurelius.faunus.io.formats;

import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 1{}[2{},3{}]
 * 1{k1=v1}[2{},3{k1=v2}]
 *
 * EASY: 1\t2,3,4,5
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusTextInputFormat extends TextInputFormat {


    private class FaunusLineReader extends LineRecordReader {

    }

}
