import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;

public class HbaseCreateTable {
      
   public static void main(String[] args) throws IOException {

      // Instantiating configuration class
      Configuration con = HBaseConfiguration.create();

      // Instantiating HbaseAdmin class
      @SuppressWarnings({ "resource", "deprecation" })
	HBaseAdmin admin = new HBaseAdmin(con);

      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("weather"));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("cf"));
      //tableDescriptor.addFamily(new HColumnDescriptor("professional"));
      System.out.println( "Connecting..." );

      // Execute the table through admin
      admin.createTable(tableDescriptor);
      System.out.println("Table created ");
   }
}