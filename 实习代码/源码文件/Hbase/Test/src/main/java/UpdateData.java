import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class UpdateData{
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args)throws IOException{
        init();
        updateData("Supermarket");
        close();
    }
    //úËÞ¥
    public static void init(){
        //ËConfiguration ùa
        configuration  = HBaseConfiguration.create();
        try{
            //9n ConfigurationùaËConnection ùa
            connection = ConnectionFactory.createConnection(configuration);
            //·Ö Admin ùa
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("Connect to HBase successfully!");
    }
    //síÞ¥
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
    //úh
    public static void createTable(String myTableName,String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("Talbe is exists, going to delete the table...");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for(String str:colFamily){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    public static void updateData(String tableName) throws IOException {
        FileReader reader = new FileReader("/root/IdeaProjects/Test/data/update.dat");
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
            table.put(put);
        }
        table.close();
        br.close();
        reader.close();
    }
}
