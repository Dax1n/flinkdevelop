package com.daxin.stream

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
  * Created by Daxin on 2017/4/19.
  */
class MySQLSink extends RichSinkFunction[Tuple2[String, Int]] {


  var connection: Connection = _;
  var preparedStatement: PreparedStatement = _;
  val username = "root";
  val password = "root";
  val drivername = "com.mysql.jdbc.Driver";
  val url = "jdbc:mysql://node:3306/log";

  /**
    * Function  for standard sink behaviour. This function is called for every record.
    * @param value The input record.
    */
  override def invoke(value: (String, Int)): Unit = {

    Class.forName(drivername);
    connection = DriverManager.getConnection(url, username, password);
    val sql = "insert into record(url,nums) values(?,?)";
    preparedStatement = connection.prepareStatement(sql);
    preparedStatement.setString(1, value._1);
    preparedStatement.setInt(2, value._2);
    preparedStatement.executeUpdate();
    if (preparedStatement != null) {
      preparedStatement.close();
    }
    if (connection != null) {
      connection.close();
    }

  }
}
