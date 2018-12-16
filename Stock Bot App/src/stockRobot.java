
import java.sql.*;
import java.util.ArrayList;

public class stockRobot implements Runnable {

    private volatile boolean runThread = true;
    public static Connection con = getConnection("database_name","database_password");
    private static String buyerName;
    private static String tickerName;
    private static double threshold;

    public static void main(String[] args) {
	// write your code here
        if (args.length <= 2) {
            System.out.println("Not given enough or any command line arguments");
            return;
        }
        buyerName = args[0];
        tickerName = args[1];
        threshold = Double.parseDouble(args[2]);
        Thread buyer = new Thread(new stockRobot());
        buyer.start();

    }

    @Override
    public void run()  {
        while (runThread) {
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                System.out.println("Trouble sleeping");
            }

            try {
                executeBuy();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }


    private  void executeBuy() throws SQLException {
        try {
            Statement stmt = null;
            ResultSet rs = null;
            ResultSet buyerResult = null;
            ResultSet rs2 = null;
            stmt = con.createStatement();
            rs = getResults(String.format("select * from SellOrder natural join CompanyInfo " +
                    "where TickerName like '%s' and Price <= %f;", tickerName, threshold));
            if (!rs.next()) {
                System.out.println("No available lots found");
                return;
            }
            rs.beforeFirst(); //Resetting the cursor so i don't miss the first row

            buyerResult = getResults(String.format("select * from Person where AccountName like '%s';", buyerName));
            buyerResult.next();
            int transID = 0;
            int sellerID = 0;
            int companyID = 0;
            int quantity = 0;
            int origQuantity = 0;
            int sharesPurchased;
            double price = 0.00;
            double buyerBalance = 0;
            buyerBalance = buyerResult.getDouble("Balance");
            int buyerID = 0;
            buyerID = buyerResult.getInt("AccountID");
            con.setAutoCommit(false);
            while (rs.next()) {
                transID = rs.getInt("TransactionID");
                sellerID = rs.getInt("AccountID");
                companyID = rs.getInt("CompanyID");
                quantity = rs.getInt("Quantity");
                origQuantity = quantity;
                price = rs.getDouble("Price");
                sharesPurchased = 0;
                for (int i = 1; i <= quantity; i++) {
                    if (buyerBalance > (price)) {
                        buyerBalance -= price;
                        sharesPurchased++;
                        quantity--;
                    } else {
                        break;
                    }

                }
                if (sharesPurchased == 0) { //couldn't afford any stocks
                    System.out.println("Terminating program due to lack of funds.");
                    runThread = false;
                    con.rollback();
                    return;
                }

                //Decreasing the balance of the buyer
                stmt.executeUpdate(String.format("update Person " +
                        "set Balance = Balance - %f " +
                        "where AccountName like '%s' LIMIT 1;", (sharesPurchased * price), buyerName));

                //Increasing the balance for the seller
                stmt.executeUpdate(String.format("update Person " +
                        "set Balance = Balance + %f " +
                        "where AccountID = %d LIMIT 1;", (sharesPurchased * price), sellerID));

                //Decreasing the stock amount from the seller
                if (quantity == 0) {
                    stmt.executeUpdate(String.format("delete from Stock " +
                            "where CompanyID = %d and AccountID = %d and Quantity = %d LIMIT 1;", companyID, sellerID, origQuantity));
                }
                else {
                    stmt.executeUpdate(String.format("update Stock set Quantity = Quantity - %d " +
                            "where CompanyID = %d and AccountID = %d and Quantity >= %d LIMIT 1;", quantity, companyID, sellerID, quantity));
                }
                // Giving the stocks purchased to the user
                stmt.executeUpdate(String.format("insert into Stock values(null,%d,%d,%d);", companyID, buyerID, quantity));

                // Deleting the sell order from the table
                stmt.executeUpdate(String.format("delete from SellOrder where TransactionID = %d;", transID));

                con.commit();
                System.out.println("Purchased lot " + transID);
                System.out.println(quantity + " shares of " + tickerName + " at $"
                        + String.format("%.2f",price) + " per share");
                System.out.println("Remaining funds " + buyerBalance);

            }


        }
        catch (Exception ex) {
            con.rollback();
            System.out.println("Rolling back!");
        }

    }




    private static Connection getConnection(String url, String userPw) {
        Connection conn = null;
        try {
            // The newInstance() call is a work around for some
            // broken Java implementations
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            ex.printStackTrace();
            // handle the error
        }

        try {
            conn = DriverManager.getConnection(url + userPw);

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
            conn = null;
        }

        return conn;

    }

    private ResultSet getResults(String query) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = con.createStatement();
            stmt.execute("use cs480fa2018");
            rs = stmt.executeQuery(query);
        }catch (SQLException ex) {
            System.out.println("SQL Exception! inside getResults");
        }
        return rs;
    }




}
