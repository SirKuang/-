import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.Scanner;

public class DeleteData{
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init(){

        configuration  = HBaseConfiguration.create();
        try{

            connection = ConnectionFactory.createConnection(configuration);

            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("Connect to HBase successfully!");
    }

    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args)throws IOException{
        init();
        System.out.println("Please input the RowID of the data you want to delete:");
        Scanner in = new Scanner(System.in);
        String input = in.nextLine();
        DeleteRow("Supermarket",input);
        System.out.println("A row is deleted!");
        close();
    }

    public static void DeleteRow(String tableName, String rowKey) {
        try {
            Table t = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            t.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}