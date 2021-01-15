//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
//import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//public class GeneratoreHFile {
//    public static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
//            String[] items = line.split(",", -1);
//            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(items[0].getBytes());
//            KeyValue kv = new KeyValue(Bytes.toBytes(items[0]),
//                    Bytes.toBytes(items[1]), Bytes.toBytes(items[2]),
//                    System.currentTimeMillis(), Bytes.toBytes(items[3]));
//            if (null != kv) {
//                context.write(rowkey, kv);
//            }
//        }
//    }
//    public static void main(String[] args) throws IOException,
//            InterruptedException, ClassNotFoundException {
//        Configuration conf = new Configuration();
//        String input = "C:\\Users\\zm\\Desktop\\HBaseFile\\input\\新建文本文档.txt";
//        String output = "C:\\Users\\zm\\Desktop\\HBaseFile\\out";
//        Job job = new Job(conf, "HFile bulk load test");
//        job.setJarByClass(GeneratoreHFile.class);
//        job.setMapperClass(HFileMapper.class);
//        job.setReducerClass(KeyValueSortReducer.class);
//        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
//        FileInputFormat.addInputPath(job, new Path(input));
//        FileOutputFormat.setOutputPath(job, new Path(output));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//
//    }
//}
