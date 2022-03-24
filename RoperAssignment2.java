/*
This is a Java skeleton code to help you out how to start this assignment.
Please keep in mind that this is NOT a compilable/runnable java file.
Please feel free to use this skeleton code.
Please give a closer look at the "To Do" parts of this file. You may get an idea of how to finish this assignment.
*/
import java.text.DecimalFormat;
import java.util.*;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//ssh -N -p922 -L4321:mysql.cs.wwu.edu:3306 roperc@proxy.cs.wwu.edu

class RoperAssignment2 {

    static class StockData {
        // To Do:
        // Create this class which should contain the information  (date, open price, high price, low price, close price) for a particular ticker

        /*
        Stock format (example)
        1. ticker symbol (MSFT),
        2. date (8/18/2004),
        3. opening price (26.93),
        4. high price (27.50),
        5. low price (26.89),
        6. closing price (27.46),
        7. volume or number of shares traded on that day (58844000 shares), and
        8. adjusted closing price (19.46).
        */

        String ticker, date;
        float open, high, low, close, adj_close;
        int volume;

        public StockData(String ticker, String date, float open, float high, float low, float close, int volume, float adj_close) {
            this.ticker = ticker;
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.adj_close = adj_close;
        }

        public StockData(StockData stock) {
            this(stock.ticker,stock.date, stock.open, stock.high, stock.low, stock.close, stock.volume, stock.adj_close);
        }

        public String getDate() {
            return date;
        }

        public float getOpen() {
            return open;
        }

        public float getClose() { return close; }

        public void adjust(float split){
            open = (open/split);
            high = (high/split);
            low = (low/split);
            close = (close/split);
        }

        public String split_check(float prev_open) {
            float dif = close / prev_open;
            String val = "";
            if (Math.abs(dif - 2.0) < 0.2) {
                val = "2:1";
            }
            if (Math.abs(dif - 3.0) < 0.3) {
                val = "3:1";
            }
            if (Math.abs(dif - 1.5) < 0.15) {
                val = "3:2";
            }
            return val;
        }
    }

    static Connection conn;
    static final String prompt = "Enter ticker symbol [start/end dates]: ";
    static DecimalFormat df = new DecimalFormat("0.00");

    public static void main(String[] args) throws Exception {
        String paramsFile = "src/readerparams.txt";
        if (args.length >= 1) {
            paramsFile = args[0];
        }

        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile));
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.printf("Database connection %s %s established.%n", dburl, username);

            Scanner in = new Scanner(System.in);
            System.out.print(prompt);
            String input = in.nextLine().trim();

            while (input.length() > 0) {
                String[] params = input.split("\\s+");
                String ticker = params[0];
                String startdate = null, enddate = null;
                if (getName(ticker)) {
                    if (params.length >= 3) {
                        startdate = params[1];
                        enddate = params[2];
                    }
                    Deque<StockData> data = getStockData(ticker, startdate, enddate);
                    System.out.println();
                    System.out.println("Executing investment strategy");
                    doStrategy(data);
                }
                else
                    System.out.printf("%s not found in database.%n", ticker);

                System.out.println();
                System.out.print(prompt);
                input = in.nextLine().trim();
            }

            // Close the database connection
            System.out.println("Database connection closed.");

        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }

    static boolean getName(String ticker) throws SQLException {
        // To Do:
        // Execute the first query and print the company name of the ticker user provided (e.g., INTC to Intel Corp.)
        // Please don't forget to use a prepared statement
        PreparedStatement pstmt = conn.prepareStatement("select Name " +
                                                            "from company " +
                                                            "where Ticker = ?" );
        pstmt.setString(1, ticker);
        ResultSet rs = pstmt.executeQuery();

        boolean ret = false;
        if(rs.next()){
            System.out.printf("%s%n", rs.getString(1));
            ret = true;
        }

        pstmt.close();
        return ret;
    }

    static Deque<StockData> getStockData(String ticker, String start, String end) throws SQLException {
        // To Do:
        // Execute the second query which will return stock information of the ticker (descending on the transaction date)
        // Please don't forget to use prepared statement

        PreparedStatement pstmt;

        if(start != null && end != null) {
            pstmt = conn.prepareStatement(
                        "select * " +
                            "from pricevolume " +
                            "where Ticker = ? " +
                            "and TransDate >= ?" +
                            "and TransDate <= ?" +
                            "order by TransDate DESC");
            pstmt.setString(1, ticker);
            pstmt.setString(2, start);
            pstmt.setString(3, end);
        }
        else
            pstmt = conn.prepareStatement(
                        "select * " +
                            "from pricevolume " +
                            "where Ticker = ? " +
                            "order by TransDate DESC");
            pstmt.setString(1, ticker);

        ResultSet rs = pstmt.executeQuery();

        Deque<StockData> result = new ArrayDeque<>();
        List<StockData> stocks = new ArrayList<>();

        // Did we get anything? If so, output data.

        // To Do:
        // Loop through all the dates of that company (descending order)
        // Find split if there is any (2:1, 3:1, 3:2) and adjust the split accordingly
        // Include the adjusted data to result (which is a Deque); You can use addFirst method for that purpose
        StockData prev = new StockData(" ", " ", 0, 0, 0, 0 ,0 ,0);
        int split_count = 0;
        int day_count = 0;
        float spg = 1;
        StockData stock;
        while(rs.next()){
            stock = new StockData(
                    rs.getString(1),
                    rs.getString(2),
                    Float.parseFloat(rs.getString(3).strip()),
                    Float.parseFloat(rs.getString(4).strip()),
                    Float.parseFloat(rs.getString(5).strip()),
                    Float.parseFloat(rs.getString(6).strip()),
                    Integer.parseInt(rs.getString(7).strip()),
                    Float.parseFloat(rs.getString(8).strip())
            );

            String split = stock.split_check(prev.getOpen());
            if(!split.equals("")){
                System.out.println(split + " split on " + stock.getDate() + "   " + df.format(stock.getClose()) + " --> " + df.format(prev.getOpen()));
                switch(split){
                    case "2:1":
                        spg *= 2;
                        break;
                    case "3:1":
                        spg *= 3;
                        break;
                    case "3:2":
                        spg *= 1.5f;
                        break;
                }
                split_count++;
            }
            prev = new StockData(stock);
            stock.adjust(spg);
            result.addFirst(stock);
            day_count++;
        }
        System.out.printf("%s in %s trading days%n", split_count, day_count);

        pstmt.close();
        return result;
    }

    static void doStrategy(Deque<StockData> data) {
        //To Do:
        // Apply Steps 2.6 to 2.10 explained in the assignment description
        // data (which is a Deque) has all the information (after the split adjustment) you need to apply these steps
        int li = 0;
        ArrayList<Float> days = new ArrayList();
        double avg;
        int trans_num = 0;
        float cash = 0;
        int shares = 0;
        float prev_close = 0;
        boolean buy = false;
        StockData d;
        while(data.peekFirst() != data.peekLast()){
            d = data.peek();

            if(li == 50){
                li = 0;
            }

            if(buy){
                cash -= (d.getOpen() * 100) + 8;
                shares += 100;
                trans_num++;
                buy = false;
            }

            if(days.size() == 50){
                avg = days.stream().mapToDouble(Float::doubleValue).sum() / 50;

                if((d.getClose() < avg) &&
                        (d.getClose() / d.getOpen() < 0.97000001)){
                    buy = true;
                }
                else if(shares >= 100 &&
                        (d.getOpen() > avg) &&
                        (d.getOpen() / prev_close > 1.00999999)){

                    cash += (((d.getOpen() + d.getClose()) / 2) * 100) - 8;
                    shares -= 100;
                    trans_num++;
                }
                days.set(li++, d.getClose());
            }

            else{
                days.add(d.getClose());
                li++;
            }
            prev_close = d.getClose();
            data.pop();
        }

        if(days.size() < 50){
            System.out.printf("Net cash: 0%n");
        }

        else {
            d = data.pop();
            cash += d.getOpen() * shares;
            System.out.printf("Transactions executed: %d%nNet cash: %s%n", trans_num, df.format(Math.round(cash * 100d) /100d));
        }
    }
}