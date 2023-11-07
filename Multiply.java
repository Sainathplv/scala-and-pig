import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;





/* this class element is to declare or intialize key,index and value.The class element code format is already written by professor in the form of Pseudo code*/
class Element implements Writable{
    public int tag;
    public int index;
    public double value;
    
    Element(){}
    Element(int tag, int index, double value){this.tag = tag; this.index = index; this.value = value;}
/* import java.io.DataInput for input reading */    
    public void write(DataOutput output) throws IOException {
           output.writeInt(tag);
           output.writeInt(index);
           output.writeDouble(value);
    }    
/* import java.io.DataInput for input reading */
    public void readFields(DataInput input) throws IOException {
           tag = input.readInt();
           index = input.readInt();
           value = input.readDouble();
    }

}
class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
    Pair ( int i, int j ) { this.i = i; this.j = j; }

    /*...*/
    public void write(DataOutput output) throws IOException {
           output.writeInt(i);
           output.writeInt(j);
    }
        
    public void readFields(DataInput input) throws IOException {
           i = input.readInt();
           j = input.readInt();
    }

    /* now i need ro return a pair such as hash code (i,j) pair i need to get its value from the input so i need to return (i,j)*/
    @Override
    public String toString() {
           return i +","+ j + ",";
    }
    
    @Override
    /* https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/WritableComparable.html (format is refered from this not the logic) */
    public int compareTo(Pair compare) {
           if(i < compare.i)
           {
              return -1;
           }
           else if(i > compare.i)
           {
              return 1;
           }
           else
           { 
               if(j < compare.j) {
                 return -1;
               } 
               else if(j > compare.j)
               {
                 return 1;
               }
           }
           return 0;
    }
                                    
}

public class Multiply extends Configured implements Tool {

    /* ... */   
    public static class MatrixMapperM extends Mapper<Object,Text,IntWritable,Element> {
         @Override     
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                /*from the psuedo code we need to get i,j,v and we need to form pairs in such a way (j,(0(matrix M),i,v) . We need to split the string and parse the values*/ 
                String[] parts = value.toString().split(",");
               
                int i = Integer.parseInt(parts[0]);
                IntWritable j = new IntWritable(Integer.parseInt(parts[1]));
                double v = Double.parseDouble(parts[2]);
                /*for element (0,i,v) create a element instance and for key value pair using context.write(j,element) */
                Element e = new Element(0, i, v); 
                context.write(j, e);
                }
    }
    public static class MatrixMapperN extends Mapper<Object,Text,IntWritable,Element> {
         @Override     
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                /*from the psuedo code we need to get i,j,v and we need to form pairs in such a way (i,(1(matrix N),j,v) . We need to split the string and parse the values*/ 
                String[] parts = value.toString().split(",");
               
                int j = Integer.parseInt(parts[1]);
                IntWritable i = new IntWritable(Integer.parseInt(parts[0]));
                double v = Double.parseDouble(parts[2]);
/*for element (1,j,v) create a element instance and for key value pair using context.write(i,element) */
                Element e = new Element(1, j, v); 
                context.write(i, e);
                }
    }    
    
    
    public static class Reducer1 extends Reducer<IntWritable,Element,Pair,DoubleWritable>{
    
         public void reduce(IntWritable key, Iterable<Element> values, Context context) throws IOException, InterruptedException {
             
                ArrayList<Element> M = new ArrayList<Element>();
                ArrayList<Element> N = new ArrayList<Element>();
                
                
                for(Element e : values) {
                    /*here o,1 are the tags that gonna represent the matrix M,N were this tells reducer from which matrix the pair has come and each and every spect need to be aggumented to the array and so declaring new element instead of just having M.add(e) which was taken reference from the join example  */                    
                    if(e.tag == 0)
                    { 
                       Element A = new Element(e.tag, e.index, e.value); 
                       M.add(A);
                    }           
                    else if(e.tag == 1)
                    {           
                       Element A = new Element(e.tag, e.index, e.value);
                       N.add(A);
                    }
                }
                
                for(int i=0;i<M.size();i++)
                {
                   for(int j=0;j<N.size();j++)
                   {  
                      context.write(new Pair(M.get(i).index,N.get(j).index), new DoubleWritable(M.get(i).value * N.get(j).value));
                    }
                 }       
         }
    }                      
    public static class Mapper2 extends Mapper<Object, Text, Pair, DoubleWritable> {
           @Override
           /* JUST returning the same key value pair back to the reducer where input for mapper gonna be the same output from reducer 1 */
           public void map(Object key, Text Value, Context context) throws IOException, InterruptedException {
                  String[] parts = Value.toString().split(",");
                  
                  
                  context.write(new Pair(Integer.parseInt(parts[0]),Integer.parseInt(parts[1])), new DoubleWritable(Double.parseDouble(parts[2])));
           }      
    }
    
    public static class Reducer2 extends Reducer<Pair, DoubleWritable, Pair,DoubleWritable> {
          @Override
          /* in reducer2 it gonna aggument all key keys and set value by addition */
          public void reduce(Pair key, Iterable<DoubleWritable> Values, Context context) throws IOException, InterruptedException {
                
                 double sum = 0.0;
                 for(DoubleWritable v : Values)
                 {
                    sum = sum + v.get();
                 }
                 
                 context.write(key, new DoubleWritable(sum));
          }
    }
                            
    public int run ( String[] args ) throws Exception {
        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
    
           Job job = Job.getInstance();
           job.setJobName("intermediate");
           job.setJarByClass(Multiply.class);
           job.setOutputKeyClass(Pair.class);
           job.setOutputValueClass(DoubleWritable.class);
           job.setMapOutputKeyClass(IntWritable.class);
           job.setMapOutputValueClass(Element.class);
           job.setReducerClass(Reducer1.class);
           job.setOutputFormatClass(TextOutputFormat.class);
           MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixMapperM.class);
           MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixMapperN.class);           
           FileOutputFormat.setOutputPath(job, new Path(args[2]));
           job.waitForCompletion(true);
           
           Job job2 = Job.getInstance();
           job2.setJobName("output");
           job2.setJarByClass(Multiply.class);
           job2.setOutputKeyClass(Pair.class);
           job2.setOutputValueClass(DoubleWritable.class);
           job2.setMapOutputKeyClass(Pair.class);
           job2.setMapOutputValueClass(DoubleWritable.class);
           job2.setMapperClass(Mapper2.class);
           job2.setReducerClass(Reducer2.class);
           job2.setInputFormatClass(TextInputFormat.class);
           job2.setOutputFormatClass(TextOutputFormat.class);
           FileInputFormat.setInputPaths(job2, new Path(args[2]));           
           FileOutputFormat.setOutputPath(job2, new Path(args[3]));
           job2.waitForCompletion(true);
           
           	
    }
}
