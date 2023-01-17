package helloworld;

import java.util.HashMap;
import java.util.Map;

import java.sql.*;
import java.util.Properties;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static Connection conn;

    static {
        try {
            final String CONNECTION_STRING = "jdbc:mysql:aws://<db-writer-instance-endpoint>:3306/<tablename>";
            final String username = "<username>";
            final String password = "<password>";


            // final Properties properties = new Properties();
            // Enable AWS IAM database authentication
            // properties.setProperty("useAwsIam", "true");
            // properties.setProperty("user", USER);
            // conn = DriverManager.getConnection(CONNECTION_STRING, properties);


            Class.forName("software.aws.rds.jdbc.mysql.Driver");
            conn = DriverManager.getConnection(CONNECTION_STRING, username, password);

            conn.setAutoCommit(false);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        final LambdaLogger logger = context.getLogger();

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent()
                .withHeaders(headers);
        try {
            // Use the connection here
            // handle any transactions and commit them here
            // String sqltable = "CREATE TABLE employees " +
            // "(id INTEGER not NULL, " +
            // " first VARCHAR(255), " +
            // " last VARCHAR(255), " +
            // " age INTEGER, " +
            // " PRIMARY KEY ( id ))";
            //try {
            //    final Statement statement = conn.createStatement();
            //    statement.executeUpdate(sqltable);
            //} catch (SQLException e) {logger.log(e.getMessage());}
            String res = "";
            try (
                    final Statement statement = conn.createStatement();
                    final ResultSet resultSet = statement.executeQuery("SELECT * FROM employees")) {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (resultSet.next()) {
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1)
                            logger.log(",  ");
                        String columnValue = resultSet.getString(i);
                        logger.log(columnValue + " " + rsmd.getColumnName(i));
                        res += columnValue;
                    }
                    logger.log("");
                }
            }
            // statement.executeUpdate(sql);

            conn.commit();

            return response
                    .withStatusCode(200)
                    .withBody(res);
        } catch (SQLException e) {
            return response
                    .withBody(e.getMessage())
                    .withStatusCode(500);
        }
    }
}