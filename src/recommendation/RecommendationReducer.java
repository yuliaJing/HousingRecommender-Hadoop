// 
 // Author - Jack Hebert (jhebert@cs.washington.edu) 
 // Copyright 2007 
 // Distributed under GPLv3 
 // 
// Modified - Dino Konstantopoulos
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 
package recommendation;

 import java.io.IOException; 
 import java.util.Iterator; 
  
 import org.apache.hadoop.io.WritableComparable; 
 import org.apache.hadoop.mapred.MapReduceBase; 
 import org.apache.hadoop.mapred.OutputCollector; 
 import org.apache.hadoop.mapred.Reducer; 
 import org.apache.hadoop.mapred.Reporter; 
 import org.apache.hadoop.io.Text; 
  
 public class RecommendationReducer extends MapReduceBase implements Reducer<WritableComparable, Text, Text, Text> { 
  
   public void reduce(WritableComparable key, Iterator values, 
                      OutputCollector output, Reporter reporter) throws IOException { 

//	   String outLinks = ""; 
	   while (values.hasNext()) { 
		   String curr = ((Text)values.next()).toString();
		   output.collect(key, new Text(curr));
	   }
	    
   } 
 } 
 