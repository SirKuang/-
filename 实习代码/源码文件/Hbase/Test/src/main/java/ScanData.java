import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class ScanData{
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
        System.out.println("init success!");
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
        /*System.out.println("Scan all data...");
        scanAllData("Supermarket");*/
        System.out.println("Scan part data1...");
        scanPartData1("Supermarket");
        /*System.out.println("Scan part data2...");
        scanPartData2("student");*/
        close();
    }
    /*public static void scanAllData(String tableName) {
        try {
            Table t = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = t.getScanner(new Scan());
            for(Result r: rs) {
                for(Cell cell: r.rawCells()) {
                    System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneValue(cell))+"\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
    public static void scanPartData1(String tableName) {
        try {
            System.out.println("Please input the ID of the data you want to scan:");
            Scanner in = new Scanner(System.in);
            String input = in.nextLine();
            Table t = connection.getTable(TableName.valueOf(tableName));
            Filter filter = new SingleColumnValueFilter("ID".getBytes(), "".getBytes(),CompareOp.EQUAL, input.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner rs = t.getScanner(scan);
            for(Result r: rs) {
                for(Cell cell: r.rawCells()) {
                    System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneValue(cell))+"\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*public static void scanPartData2(String tableName) {
        try {
            Table t = connection.getTable(TableName.valueOf(tableName));
            Filter filter1 = new SingleColumnValueFilter("Course".getBytes(), "Math".getBytes(), CompareOp.GREATER,"85".getBytes());
            Filter filter2 = new SingleColumnValueFilter("Course".getBytes(), "Chinese".getBytes(), CompareOp.LESS, "85".getBytes());
            ArrayList<Filter> listForFilters = new ArrayList<Filter>();
            listForFilters.add(filter1);
            listForFilters.add(filter2);
            Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,listForFilters);
            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner rs = t.getScanner(scan);
            for(Result r: rs) {
                for(Cell cell: r.rawCells()) {
                    System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneValue(cell))+"\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
}