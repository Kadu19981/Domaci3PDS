package pds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapReduceTemp {

    public static class AvgTuple implements Writable {

        
        private int minSum=0;
        private int minBrojac=0;
        private int maxSum=0;
        private int maxBrojac=0;

    
        public int getMinSum() {
			return minSum;
		}
		public void setMinSum(int minSum) {
			this.minSum = minSum;
		}
		public int getMinBrojac() {
			return minBrojac;
		}
		public void setMinBrojac(int minBrojac) {
			this.minBrojac = minBrojac;
		}
		public int getMaxSum() {
			return maxSum;
		}
		public void setMaxSum(int maxSum) {
			this.maxSum = maxSum;
		}
		public int getMaxBrojac() {
			return maxBrojac;
		}
		public void setMaxBrojac(int maxBrojac) {
			this.maxBrojac = maxBrojac;
		}
		@Override
        public void readFields(DataInput in) throws IOException {
			minSum = in.readInt();
			minBrojac = in.readInt();
			maxSum = in.readInt();
			maxBrojac = in.readInt();
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(minSum);
            out.writeInt(minBrojac);
            out.writeInt(maxSum);
            out.writeInt(maxBrojac);
        }

        public String toString() {
            return "MinSrednje: " + (1.0 * minSum/minBrojac) + ", MaxSrednje: " + (1.0 * maxSum/maxBrojac);
        }

    }


    public static class TempMapper extends Mapper<Object, Text, Text, AvgTuple> {
        private Text month = new Text();
        private AvgTuple outTuple = new AvgTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            month.set(line[1].substring(4,6));
            int temperature = Integer.parseInt(line[3]);

           
            String extreme = line[2];
            if(extreme.equals("TMIN")){
                outTuple.setMinSum(temperature);
                outTuple.setMinBrojac(1);
            }else if(extreme.equals("TMAX")){
                outTuple.setMaxSum(temperature);
                outTuple.setMaxBrojac(1);
            }

            context.write(month, outTuple);
        }
    }

    public static class TempReducer extends Reducer<Text, AvgTuple, Text, AvgTuple> {

        private AvgTuple resultTuple = new AvgTuple();

        public void reduce(Text key, Iterable<AvgTuple> tuples, Context context) throws IOException, InterruptedException {
            int minSum = 0;
            int maxSum = 0;
            int minBrojac = 0;
            int maxBrojac = 0;

            for(AvgTuple tup : tuples){
                minSum += tup.getMinSum();
                maxSum += tup.getMaxSum();
                minBrojac+= tup.getMinBrojac();
                maxBrojac += tup.getMaxBrojac();
            }

            resultTuple.setMinSum(minSum);
            resultTuple.setMinBrojac(minBrojac);
            resultTuple.setMaxSum(maxSum);
            resultTuple.setMaxBrojac(maxBrojac);

            context.write(key, resultTuple);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "srednje ekstremna temp");
        job.setJarByClass(MapReduceTemp.class);
        job.setMapperClass(TempMapper.class);
        job.setCombinerClass(TempReducer.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}