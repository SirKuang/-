import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class AddData{
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args)throws IOException{
        init();
        putData("Supermarket");
        close();
    }

    public static void init(){

        configuration  = HBaseConfiguration.create();
        try{

            connection = ConnectionFactory.createConnection(configuration);

            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
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
    public static void putData(String tableName) throws IOException {
        FileReader reader = new FileReader("/root/IdeaProjects/Test/data/add.dat");
        BufferedReader br = new BufferedReader(reader);
        String str = null;
        Table table = connection.getTable(TableName.valueOf(tableName));
        while((str = br.readLine()) != null) {
            System.out.println("processing line:" + str);
            String[] cols = str.split(",");
            Put put = new Put(cols[0].getBytes());
            put.addColumn("ID".getBytes(), "".getBytes(), cols[1].getBytes());
            put.addColumn("Name".getBytes(), "".getBytes(), cols[2].getBytes());
            put.addColumn("Type".getBytes(), "".getBytes(), cols[3].getBytes());
            put.addColumn("Price".getBytes(), "".getBytes(), cols[4].getBytes());
            put.addColumn("Place".getBytes(), "".getBytes(), cols[5].getBytes());
            put.addColumn("Weight".getBytes(), "".getBytes(), cols[6].getBytes());
            /*put.addColumn("Course".getBytes(), "Chinese".getBytes(), cols[6].getBytes());*/
            table.put(put);
        }
        table.close();
        br.close();
        reader.close();
    }
}