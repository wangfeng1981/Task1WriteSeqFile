/// write a sequence file into hdfs
package com.company;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.ByteArray;


public class Main {

    public static byte[] short2ByteArray(short sval)
    {
        ByteBuffer bb = ByteBuffer.allocate(2) ;
        bb.putShort(sval) ;
        return bb.array() ;
    }

    public static void main(String[] args) throws Exception{
	// write your code here
        if( args.length == 0 ) System.exit(11);
        System.out.println("Try to write seqfile:" + args[0]);
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf) ;
        Path outfile = new Path(args[0]) ;
        SequenceFile.Writer writer = SequenceFile.createWriter(
                conf
                ,Writer.file(outfile)
                ,Writer.keyClass(IntWritable.class)
                ,Writer.valueClass(BytesWritable.class)
                ,Writer.compression(SequenceFile.CompressionType.NONE)
        ) ;
        int imageSize = 2748 * 2748 ;
        byte[] bytesDataArr = new byte[imageSize * 2] ;
        BytesWritable bytesWritable = new BytesWritable() ;
        for( int i = 0 ; i < 288 ; ++ i )
        {//288 obs per day.
            long offset0 = writer.getLength() ;
            byte[] oneByte = short2ByteArray( (short)i ) ;
            for(int px =0 ; px<imageSize ; ++ px )
            {
                bytesDataArr[px*2+0] = oneByte[0] ;
                bytesDataArr[px*2+1] = oneByte[1] ;
            }
            bytesWritable.set( bytesDataArr , 0 , imageSize*2 );
            writer.append( new IntWritable(i) , bytesWritable);
            long offset1 = writer.getLength() ;
            System.out.println( String.valueOf(i) + " from " + String.valueOf(offset0)
                + " to " + String.valueOf(offset1)
            );
        }
        if( writer != null )
        {
            writer.close() ;
        }

    }
}
