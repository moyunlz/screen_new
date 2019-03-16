package com.example;

import java.util.Random;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.lang.Math;

// NOTE:
// reference: https://www.javatips.net/blog/h2-in-memory-database-example

public class MyTask {

    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) throws Exception {
        try {
            calculate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void calculate() throws SQLException {

        int n = 100000;  // the total random number need to generate
        int number_of_group = n / 1000;
        Random r = new Random(1);
        double[] number = new double[n];  // store the generate numbers
        for(int i = 0; i < n; i++){
            number[i] = r.nextGaussian();
        }


        Connection connection = getDBConnection();
        PreparedStatement createPreparedStatement = null;
        PreparedStatement insertPreparedStatement = null;
        PreparedStatement selectPreparedStatement = null;

        String CreateQuery = "CREATE TABLE Data(id int primary key, groupID int,NUM DOUBLE )";  //every 1000 numbers becomes a group, groupID start from 1.
        String InsertQuery = "INSERT INTO Data" + "(id, groupID, NUM) values" + "(?,?,?)";
        //String SelectQuery = "select AVG(NUM) AS mean from Data group by groupID";
        String SelectQuery = "select STDDEV(mean) from (select AVG(NUM) AS mean from Data group by groupID)";

        connection.setAutoCommit(false);

        try {

            //create table1
            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();

            // insert
            insertPreparedStatement = connection.prepareStatement(InsertQuery); //precompile
            int group = 1,count=1;
            double num;
            String ss;
            for(int i = 0; i < n; i++) {  // insert each random number and corresponding groupID into the in-memory database
                num = number[i];
                ss = String.valueOf(num);
                insertPreparedStatement.setInt(1, i + 1);  //insert the value index
                insertPreparedStatement.setInt(2, group);  //insert the groupID
                insertPreparedStatement.setString(3, ss);  //insert the Random number
                insertPreparedStatement.executeUpdate();
                count++;
                if(count > 1000){ //reset the count to 1 when a group has been filled up
                    group+=1;
                    count=1;
                }

            }
            insertPreparedStatement.close();


            //select
            selectPreparedStatement = connection.prepareStatement(SelectQuery);
            ResultSet rs = selectPreparedStatement.executeQuery();  // the result, average for each group
            double sigma = 0.0;
            while(rs.next()){
                sigma = rs.getDouble(1);
            }

            // ****** Following is old version *****
            //double sigma = rs.getDouble(1);
            /*double[] out = new double[number_of_group];
            int i = 0;
            double sum = 0, square_sum = 0, average;
            while (rs.next()) {
                out[i] = rs.getDouble(1);
                sum = sum + out[i];
                i++;
            }

            // calculate the sum of squares
            average = sum/ (number_of_group);
            for(int j = 0; j < number_of_group; j++){
                square_sum += (out[j] - average) * (out[j] - average);
            }

            double sigma = Math.sqrt(square_sum / (number_of_group - 1));  // the standard error for group means*/
            System.out.println("estimated standard deviation");
            System.out.println("---------");
            System.out.println(sigma);
            selectPreparedStatement.close();
            connection.commit();


        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }

    }

    private static Connection getDBConnection() {
        Connection dbConnection = null;
        try { //initialize the H2 database driver
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {  //connection to database
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }
}
