// Author - Haoan Yan, Yuting Jing

package recommendation;


import java.io.File;
import java.io.IOException; 
  
 import org.apache.hadoop.io.Writable; 
 import org.apache.hadoop.io.WritableComparable; 
 import org.apache.hadoop.mapred.MapReduceBase; 
 import org.apache.hadoop.mapred.Mapper; 
 import org.apache.hadoop.mapred.OutputCollector; 
 import org.apache.hadoop.mapred.Reporter; 
 import org.apache.hadoop.io.Text; 
  
  
 public class RecommendationMapper extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text> { 
  
   public void map(WritableComparable key, Writable value, 
                   OutputCollector output, Reporter reporter) throws IOException { 
	   
	   
	   String line = ((Text)value).toString(); 
	   line = cleanString(line);
	   String temp[] = line.split(",");
	   String data[] = line.split(",");
	   if(data.length != 16) {
		   System.out.println("skip!!!!!");
		   return;
	   }
	   data[0] = temp[0];
	   int cur = 1;
//	   String nameTmp = "";
//	   System.out.println("temp1"+temp[1]);
//	   String curStr = temp[cur];
//	   if(temp[1].substring(0, 1).equals("\"")) {
//		   
//		   while(true) {
//			   curStr = temp[cur];
//			   System.out.println(curStr);
//			   
//			   if(!curStr.substring(curStr.length() - 1).equals("\"")) {
//				   cur ++;
//				   nameTmp += curStr;
//			   }
//			   else {
//				   System.out.println("break! cur="+cur);
//				   cur ++;
//				   nameTmp += curStr;
//				   break;
//			   }
//		   }
//		   
//	   }
//	   else {
//		   nameTmp = temp[1];
//		   cur ++;
//	   }
//	   
//	   data[1] = nameTmp;
//	   int dataCur = 2;
//	   for(int i = cur; i < temp.length; i++) {
//		   System.out.println(dataCur+","+i);
//		   data[dataCur++] = temp[cur];
//	   }
	   
	   
	   //16 columes in input data
	   String id = data[0];
	   String name = data[1];
	   String hostId = data[2];
	   String hostName = data[3];
	   String neighbourhood_group = data[4];
	   String neighbourhood = data[5];
	   String latitude = data[6];
	   String longitude = data[7];
	   String roomType = data[8];
	   String price = data[9];
	   String minimunNight = data[10];
	   String numberOfReviews = data[11];
	   String lastReview = data[12];
	   String reviewsPerMonth = data[13];
	   String calculatedHostListingsCount = data[14];
	   String availability365 = data[15];
	   Double targetLat = 42.429030;
	   Double targetLong = -71.077185;
	   //42.429030, -71.077185
	   Double score = getDistance(targetLat, targetLong, latitude, longitude);
	   output.collect(new Text(score.toString()),new Text((id+","+name+","+latitude+","+longitude))); 
   } 
   
   public static String cleanString(String str) {
   	String ori = "";
   	String cur = str;
   	String reg = "\"(.*?)(,)(.*?)\"";
   	cur = cur.replaceFirst("\"(\"\")","\"'");
   	cur = cur.replaceAll("\"\"", "'");
   	while(!ori.equals(cur)) {  
   		ori = cur;
   		cur = ori.replaceAll(reg,"\"$1|$3\"");
   	}
   	return cur;
   }
   
   public static Double getDistance(Double tagetLat, Double targetLong, String curLat, String curLong){
	   Double lat_d = new Double(curLat);
	   Double long_d = new Double(curLong);
	   return (lat_d - tagetLat) * (lat_d - tagetLat) + (long_d - targetLong) * (long_d - targetLong);
   }
   
 } 
 