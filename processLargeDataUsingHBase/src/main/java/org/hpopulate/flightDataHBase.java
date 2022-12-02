package org.hpopulate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class flightDataHBase {
    public static void main(String[] args) throws Exception {
        //read csv with bufferedreader -> read line by line, parse with dataparser,
        Configuration con = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(con);
        HBaseAdmin ad = (HBaseAdmin) connection.getAdmin();
        HTableDescriptor table = new
                HTableDescriptor(TableName.valueOf("hpopulate"));

        Boolean tableAlreadyExist = ad.isTableAvailable(TableName.valueOf("hpopulate"));
        if (tableAlreadyExist) {
            ad.disableTable(TableName.valueOf("hpopulate"));
            ad.deleteTable(TableName.valueOf("hpopulate"));
        }

        // Adding column families to table descriptor
        table.addFamily(new HColumnDescriptor("flightData"));
        // Execute the table through admin
        ad.createTable(table);
        Table t = connection.getTable(TableName.valueOf("hpopulate"));
        System.out.println(" Table created ");

        BufferedReader inputFile = new BufferedReader(new FileReader(args[0]));
        String line;
        int tag = 0;
        int batchMax = 100000;
        ArrayList<Put> elements = new ArrayList<Put>();

        while ((line = inputFile.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            tag += 1;
            dataParser parser = new dataParser();
            parser.parse(line);
            String key;
            if (parser.isValidFlight()) {
                key = tag + "," + parser.getAirlineID() + "," + parser.getYear() + "1";
            } else {
                key = tag + "," + parser.getAirlineID() + "," + parser.getYear() + "0";
            }
            Put p = new Put(Bytes.toBytes(key));
            p.addColumn(Bytes.toBytes("flightData"), Bytes.toBytes("AirlineID"), Bytes.toBytes(parser.getAirlineID()));
            elements.add(p);
            p.addColumn(Bytes.toBytes("flightData"), Bytes.toBytes("Year"), Bytes.toBytes(parser.getYear()));
            elements.add(p);
            p.addColumn(Bytes.toBytes("flightData"), Bytes.toBytes("Month"), Bytes.toBytes(parser.getMonth()));
            elements.add(p);
            p.addColumn(Bytes.toBytes("flightData"), Bytes.toBytes("ArrDelayMinutes"), Bytes.toBytes(parser.getArrDelayMinutes()));
            elements.add(p);
            p.addColumn(Bytes.toBytes("flightData"), Bytes.toBytes("Cancelled"), Bytes.toBytes(parser.getCancelled()));
            elements.add(p);
            p.addColumn(Bytes.toBytes("flightData"), Bytes.toBytes("Diverted"), Bytes.toBytes(parser.getDiverted()));
            elements.add(p);
            if (tag == batchMax) {
                System.out.println("one batch completed");
                t.put(elements);
                tag = 0;
                elements.clear();
            }
        }


//        // Instantiating Get class
//        Get g = new Get(Bytes.toBytes("row1"));
//
//        // Reading the data
//        Result result = t.get(g);
//
//        // Reading values from Result class object
//        byte [] value = result.getValue(Bytes.toBytes("flightData"),Bytes.toBytes("AirlineID"));
//
//        byte [] value1 = result.getValue(Bytes.toBytes("flightData"),Bytes.toBytes("ArrDelayMinutes"));
//
//        // Printing the values
//        String name = Bytes.toString(value);
//        String city = Bytes.toString(value1);
//
//        System.out.println("name: " + name + " city: " + city);
        t.close();
        connection.close();
    }
}