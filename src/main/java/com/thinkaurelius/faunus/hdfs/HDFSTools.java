package com.thinkaurelius.faunus.hdfs;

import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HDFSTools {

    private static final String FOWARD_SLASH = "/";
    private static final String FOWARD_SLASH_ASTERISK = "/*";
    private static final String DASH = "-";
    private static final String ASTERISK = "*";
    private static final String UNDERSCORE = "_";

    protected HDFSTools() {
    }

    public static List<Path> getAllFilePaths(final FileSystem fs, String path) throws IOException {
        if (null == path) path = fs.getHomeDirectory().toString();
        if (path.equals(FOWARD_SLASH)) path = "";

        final List<Path> paths = new ArrayList<Path>();

        for (final FileStatus status : fs.globStatus(new Path(path + FOWARD_SLASH_ASTERISK))) {
            final Path next = status.getPath();
            if (!next.getName().startsWith(UNDERSCORE)) {
                if (fs.isFile(next))
                    paths.add(next);
                else
                    paths.addAll(getAllFilePaths(fs, next.toString()));
            }
        }
        return paths;
    }

    public static Path getOutputsFinalJob(final FileSystem fs, String output) throws IOException {
        int largest = -1;
        for (FileStatus status : fs.listStatus(new Path(output))) {
            final String[] name = status.getPath().getName().split(DASH);
            if (name.length == 2 && name[0].equals(Tokens.JOB)) {
                if (Integer.valueOf(name[1]) > largest)
                    largest = Integer.valueOf(name[1]);
            }
        }
        if (largest == -1)
            return new Path(output);
        else
            return new Path(output + "/" + Tokens.JOB + "-" + largest);
    }

    public static void decompressJobData(final FileSystem fs, String path, String dataType, String compressFileSuffix, final CompressionCodec codec) throws IOException {
        path = new Path(path).toString();
        for (final FileStatus status : fs.globStatus(new Path(path + FOWARD_SLASH + dataType + ASTERISK + compressFileSuffix))) {
            HDFSTools.decompressFile(fs, status.getPath().toString(), path + FOWARD_SLASH + status.getPath().getName().split("\\.")[0], codec);
        }

        // delete compressed data
        for (FileStatus temp : fs.globStatus(new Path(path + FOWARD_SLASH + dataType + ASTERISK + compressFileSuffix))) {
            fs.delete(temp.getPath(), true);
        }
    }

    public static void decompressPath(final FileSystem fs, final String in, final String out, final String compressedFileSuffix, final CompressionCodec codec) throws IOException {
        final Path inPath = new Path(in);
        if (fs.isFile(inPath))
            HDFSTools.decompressFile(fs, in, out, codec);
        else {
            final Path outPath = new Path(out);
            if (!fs.exists(outPath))
                fs.mkdirs(outPath);
            for (final FileStatus status : fs.listStatus(new Path(in))) {
                if (status.getPath().getName().contains("." + compressedFileSuffix))
                    HDFSTools.decompressFile(fs, status.getPath().toString(), outPath.toString() + FOWARD_SLASH + status.getPath().getName().split("\\.")[0], codec);
            }
        }
    }

    public static void decompressFile(final FileSystem fs, final String inFile, final String outFile, final CompressionCodec codec) throws IOException {
        final OutputStream out = fs.create(new Path(outFile));
        final InputStream in = codec.createInputStream(fs.open(new Path(inFile)));
        int data;
        while (-1 != (data = in.read())) {
            out.write(data);
        }
        out.close();
        in.close();

    }
}
