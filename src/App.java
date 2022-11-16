import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class App {
    public static void main(String[] args) {
        DbConfig config = ConfigReader.read("auth.cfg");
        // initDb(config.getUsername(), config.getPassword());
        printReportOptions();
        runAnalytics(Analytics.init(config.getUsername(), config.getPassword()));
    }

    private static void initDb(String username, String password) {
        ExecutionTimer.measure(() ->  
            new DbStarter(SqlServerUtils.connectionUrl(username, password)).loadTables());
    }

    private static void runAnalytics(Analytics analytics) {
        Scanner scanner = new Scanner(System.in);
        String line = scanner.nextLine();
        while (line != null && !line.equals("q")) {
            String[] parts = line.trim().split(Delimiter.SPACE);
            String key = parts[0];
            analytics.executeQuery(key);
            line = scanner.nextLine();
        }
        scanner.close();
    }

    private static void printReportOptions() {
        System.out.println("Options");
        System.out.println("a - Get this thing");
        System.out.println("b - Get that thing");
        System.out.println("c - Get that thing");
        System.out.println("q - End");
    }
}

// Queries
final class Analytics {

    private long startTime = System.currentTimeMillis();
    private final Map<String, String> cache = new HashMap<>();

    private final Map<String, Runnable> registry = new HashMap<>() {{
        put("a", () -> query1());
        put("b", () -> query2());
        put("c", () -> query3());
    }};

    private static class Query {
        static final String 

        Q1 = "select top 10000 * from libraries",
        
        Q2 = "select top 10000 * from schools",
        
        Q3 = "select top 10000 * from schools " +
            "left join states on schools.state_code = states.state_code";

            
    }

    private final Connection connection;

    private Analytics(Connection connection) {
        this.connection = connection;
    }

    // The only function exposed to the user
    public void executeQuery(String key) {
        ExecutionTimer.measure(() -> {
            registry
            .getOrDefault(key, () -> System.out.println("The key is not found"))
            .run();
        });
    }

    public static Analytics init(String username, String password) {
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(SqlServerUtils.connectionUrl(username, password));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (connection == null) {
            throw new RuntimeException("Failed to set up connection to SQL Server");
        }

        return new Analytics(connection);
    }

    private void query3() {
        System.out.println("Q3");

        if (applyCacheIfPresent("c")) return;

        try {
            PreparedStatement statement = connection.prepareStatement(Query.Q3);
            // statement.setInt(1, Integer.parseInt(id));
            ResultSet resultSet = statement.executeQuery();

            // System.out.println("| First | Last | AID |");
            // System.out.println("----------------------");
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                String result = resultSet.getString("school_name");
                result += " | " + resultSet.getInt("state_code");
                results.add(result);
                System.out.println(result);
                // String result = "| " +
                // 		resultSet.getString("first") + " | " +
                // 		resultSet.getString("last") + " | " +
                // 		resultSet.getInt("aid") +
                // 		" |";
                // System.out.println(result);
            }
            cache.put("c", results.stream().collect(Collectors.joining(Delimiter.NEW_LINE)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void query1() {
        System.out.println("Q1");

        if (applyCacheIfPresent("a")) return;

        try {
            PreparedStatement statement = connection.prepareStatement(Query.Q1);
            // statement.setInt(1, Integer.parseInt(id));
            ResultSet resultSet = statement.executeQuery();

            // System.out.println("| First | Last | AID |");
            // System.out.println("----------------------");
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                String result = resultSet.getString("library_name");
                results.add(result);
                System.out.println(result);
                // String result = "| " +
                // 		resultSet.getString("first") + " | " +
                // 		resultSet.getString("last") + " | " +
                // 		resultSet.getInt("aid") +
                // 		" |";
                // System.out.println(result);
            }
            cache.put("a", results.stream().collect(Collectors.joining(Delimiter.NEW_LINE)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean applyCacheIfPresent(String key) {
        ensureCacheRefreshed();
        String data = cache.getOrDefault(key, null);
        boolean result = data != null;
        if (result) System.out.println(data);
        return result;
    }

    private void ensureCacheRefreshed() {
        if (getElapsedTimeInMinutes() > 0.2) {
            cache.clear();
            startTime = System.currentTimeMillis();
        }
    }

    private double getElapsedTimeInMinutes() {
        double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.println("Elapsed: " + elapsed / 60);
        return elapsed / 60;
    }

    private void query2() {
        System.out.println("Q2");

        if (applyCacheIfPresent("b")) return;

		try {
			PreparedStatement statement = connection.prepareStatement(Query.Q2);
			// statement.setInt(1, Integer.parseInt(id));
			ResultSet resultSet = statement.executeQuery();

			// System.out.println("| First | Last | AID |");
			// System.out.println("----------------------");
            List<String> results = new ArrayList<>();
			while (resultSet.next()) {
                String result = resultSet.getString("school_name");
                results.add(result);
                System.out.println(result);
				// String result = "| " +
				// 		resultSet.getString("first") + " | " +
				// 		resultSet.getString("last") + " | " +
				// 		resultSet.getInt("aid") +
				// 		" |";
				// System.out.println(result);
			}
            cache.put("b", results.stream().collect(Collectors.joining(Delimiter.NEW_LINE)));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
    }
}

// Data
final class DbStarter {

    private String connectionUrl;
    private TableSeedersExecutor executor;
    
    public DbStarter(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        this.executor = new TableSeedersExecutor(connectionUrl);
    }

    public void loadTables() {
        createTables();
        executor.run();
    }

    private void createTables() {
        String[] sql = SqlReader.read("command.sql");

        if (sql.length == 0) return;

        try {
            Connection connection = DriverManager.getConnection(connectionUrl);
            Statement command = connection.createStatement();

            for(int i = 0; i < sql.length; i++) {
                if(sql[i].trim().equals("")) continue;
                command.executeUpdate(sql[i]);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }   
}

// Db Seeders
final class TableSeedersExecutor {
    
    private final Map<String, TableSeeder> registry = new HashMap<>();

    public TableSeedersExecutor(String connectionUrl) {
        registerTableSeeders(connectionUrl);
    }

    private void registerTableSeeders(String connectionUrl) {
        registry.put(Table.T_STATES, new StatesSeeder(connectionUrl, "states.txt", Table.T_STATES));
        registry.put(Table.T_SCHOOLS, new SchoolsSeeder(connectionUrl, "schools.txt", Table.T_SCHOOLS));
        registry.put(Table.T_COUNTIES, new CountiesSeeder(connectionUrl, "county.csv", Table.T_COUNTIES));
        registry.put(Table.T_OPERATING_REVENUES, new OperatingRevenuesSeeder(connectionUrl, "operating_revenues.txt", Table.T_OPERATING_REVENUES));
        registry.put(Table.T_CAPITAL_REVENUES, new CapitalRevenuesSeeder(connectionUrl, "capital_revenues.txt", Table.T_CAPITAL_REVENUES));
        registry.put(Table.T_COLLECTION_EXPENDITURES, new CollectionExpendituresSeeder(connectionUrl, "collection_expenditures.txt", Table.T_COLLECTION_EXPENDITURES));
        registry.put(Table.T_EMPLOYEE_EXPENDITURES, new EmployeeExpendituresSeeder(connectionUrl, "employee_expenditures.txt", Table.T_EMPLOYEE_EXPENDITURES));
        registry.put(Table.T_STAFF_MEMBERS_COUNTS, new StaffMembersCountsSeeder(connectionUrl, "staff_members_counts.txt", Table.T_STAFF_MEMBERS_COUNTS));
        registry.put(Table.T_DATABASES_COUNTS, new DatabasesCountsSeeder(connectionUrl, "databases_counts.txt", Table.T_DATABASES_COUNTS));
        registry.put(Table.T_LIBRARIES, new LibrariesSeeder(connectionUrl, "library.txt", Table.T_LIBRARIES));
    }

    public void run() {
        System.out.println("We are loading the best data for you !");
        System.out.println("------ ------ ------ ------ ------ ------ ------ ------ ------ ------");
        registry.get(Table.T_STATES).seed();
        System.out.print("---- o ");
        registry.get(Table.T_SCHOOLS).seed();
        System.out.print("---- o ");
        registry.get(Table.T_COUNTIES).seed();
        System.out.print("---- o ");
        registry.get(Table.T_OPERATING_REVENUES).seed();
        System.out.print("---- o ");
        registry.get(Table.T_CAPITAL_REVENUES).seed();
        System.out.print("---- o ");
        registry.get(Table.T_COLLECTION_EXPENDITURES).seed();
        System.out.print("---- o ");
        registry.get(Table.T_EMPLOYEE_EXPENDITURES).seed();
        System.out.print("---- o ");
        registry.get(Table.T_STAFF_MEMBERS_COUNTS).seed();
        System.out.print("---- o ");
        registry.get(Table.T_DATABASES_COUNTS).seed();
        System.out.print("---- o ");
        registry.get(Table.T_LIBRARIES).seed();
        System.out.print("---- o ");
    }
}

abstract class TableSeeder {

    protected final String connectionUrl, source, table;

    public TableSeeder(String connectionUrl, String source, String table) {
        this.connectionUrl = connectionUrl;
        this.source = source;
        this.table = table;
    }

    public void seed() {
        try {
            Connection connection = DriverManager.getConnection(connectionUrl);
            
            if (isTableSeeded(connection, table)) return;

            BufferedReader reader = new BufferedReader(new FileReader(source));
            reader.readLine();

            String record = "";
            while ((record = reader.readLine()) != null) {
                insert(connection, record);
            }

            reader.close();
            connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected boolean isTableSeeded(Connection connection, String table) throws SQLException {
        Statement selection = connection.createStatement();
        ResultSet result = selection.executeQuery("select * from " + table);
        return result.next();
    }

    protected abstract void insert(Connection connection, String record) throws SQLException;
}

final class CapitalRevenuesSeeder extends TableSeeder {

    public CapitalRevenuesSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.SPACE);
        PreparedStatement insertion = connection.prepareStatement(Insertion.CAPITAL_REVENUES);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setDouble(2, Double.parseDouble(cells[1]));
        insertion.setDouble(3, Double.parseDouble(cells[2]));
        insertion.setDouble(4, Double.parseDouble(cells[3]));
        insertion.setDouble(5, Double.parseDouble(cells[4]));
        insertion.executeUpdate();
        insertion.close();
    } 
}

final class CollectionExpendituresSeeder extends TableSeeder {

    public CollectionExpendituresSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.SPACE);
        PreparedStatement insertion = connection.prepareStatement(Insertion.COLLECTION_EXPENDITURES);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setDouble(2, Double.parseDouble(cells[1]));
        insertion.setDouble(3, Double.parseDouble(cells[2]));
        insertion.setDouble(4, Double.parseDouble(cells[3]));
        insertion.executeUpdate();
        insertion.close();
    }
}

final class CountiesSeeder extends TableSeeder {

    public CountiesSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.COMMA);
        PreparedStatement insertion = connection.prepareStatement(Insertion.COUNTIES);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setInt(2, Integer.parseInt(cells[1]));
        insertion.setInt(3, Integer.parseInt(cells[2]));
        insertion.setString(4, cells[3]);
        insertion.executeUpdate();
        insertion.close();
    }
}

final class DatabasesCountsSeeder extends TableSeeder {

    public DatabasesCountsSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.SPACE);
        PreparedStatement insertion = connection.prepareStatement(Insertion.DATABASES_COUNTS);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setInt(2, Integer.parseInt(cells[1]));
        insertion.setInt(3, Integer.parseInt(cells[2]));
        insertion.executeUpdate();
        insertion.close();
    }
}

final class EmployeeExpendituresSeeder extends TableSeeder {

    public EmployeeExpendituresSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.SPACE);
        PreparedStatement insertion = connection.prepareStatement(Insertion.EMPLOYEE_EXPENDITURES);
        insertion.setInt(1, Integer.parseInt(cells[0]));

        if (cells.length > 1) {
            insertion.setDouble(2, Double.parseDouble(cells[1]));
            insertion.setDouble(3, Double.parseDouble(cells[2]));
        } else {
            insertion.setDouble(2, 0);
            insertion.setDouble(3, 0);
        }

        insertion.executeUpdate();
        insertion.close();
    }
}

final class LibrariesSeeder extends TableSeeder {

    public LibrariesSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.TAB);
        PreparedStatement insertion = connection.prepareStatement(Insertion.LIBRARIES);
        insertion.setString(1, cells[0]);
        insertion.setString(2, cells[1]);
        insertion.setString(3, cells[2]);
        insertion.setString(4, cells[3]);
        insertion.setInt(5, Integer.parseInt(cells[4]));
        insertion.setDouble(6, Double.parseDouble(cells[5]));
        insertion.setDouble(7, Double.parseDouble(cells[6]));
        
        insertion.setInt(8, Integer.parseInt(cells[13]));
        insertion.setInt(9, Integer.parseInt(cells[14]));

        insertion.setInt(10, Integer.parseInt(cells[7]));
        insertion.setInt(11, Integer.parseInt(cells[8]));
        insertion.setInt(12, Integer.parseInt(cells[9]));
        insertion.setInt(13, Integer.parseInt(cells[10]));
        insertion.setInt(14, Integer.parseInt(cells[11]));
        insertion.setInt(15, Integer.parseInt(cells[12]));

        insertion.executeUpdate();
        insertion.close();
    }
}

final class OperatingRevenuesSeeder extends TableSeeder {

    public OperatingRevenuesSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.SPACE);
        PreparedStatement insertion = connection.prepareStatement(Insertion.OPERATING_REVENUES);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setDouble(2, Double.parseDouble(cells[1]));
        insertion.setDouble(3, Double.parseDouble(cells[2]));
        insertion.setDouble(4, Double.parseDouble(cells[3]));
        insertion.setDouble(5, Double.parseDouble(cells[4]));
        insertion.executeUpdate();
        insertion.close();
    }
}

final class SchoolsSeeder extends TableSeeder {

    public SchoolsSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    public void seed() {
        try {
            Connection connection = DriverManager.getConnection(connectionUrl);
            
            if (isTableSeeded(connection, table)) return;

            new SchoolsReader(source).read().forEach(school -> {
                try {
                    insert(connection, school);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

            connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.COMMA);
        PreparedStatement insertion = connection.prepareStatement(Insertion.SCHOOLS);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setString(2, cells[1]);
        insertion.setInt(3, Integer.parseInt(cells[2]));
        insertion.executeUpdate();
        insertion.close();
    }

    private static class SchoolsReader {
        private final String source;

        public SchoolsReader(String source) {
            this.source = source;
        }

        public List<String> read() {
            List<String> schools = new ArrayList<>();
            Map<String, String> states = readStates(); // // NAME -> AA
            Map<String, String> stateCodes = readStateCodes(); // AA - 1
            try {
                BufferedReader reader = new BufferedReader(new FileReader(source));
                reader.readLine();
                String record = "";
                String state = "";
                while ((record = reader.readLine()) != null) {
                    String[] cells = record.trim().split(Delimiter.TAB);
                    if (cells.length == 1) {
                        state = cells[0];
                    } else {
                        String stateCode = stateCodes.get(states.get(state));
                        if (stateCode == null) continue;
                        String school = cells[0] + Delimiter.COMMA + cells[1] + Delimiter.COMMA + stateCodes.get(states.get(state));
                        schools.add(school);
                    }
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            return schools;
        }
    
        private Map<String, String> readStates() {
            Map<String, String> states = new HashMap<>();
            try {
                BufferedReader reader = new BufferedReader(new FileReader("state_code_and_names.txt"));
                String record = "";
                while ((record = reader.readLine()) != null) {
                    String[] cells = record.trim().split(Delimiter.TAB);
                    states.put(cells[1].toUpperCase(), cells[0]);
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    
            return states;
        }

        private Map<String, String> readStateCodes() {
            Map<String, String> stateCodes = new HashMap<>();
            try {
                BufferedReader reader = new BufferedReader(new FileReader("states.txt"));
                String record = "";
                reader.readLine();
                while ((record = reader.readLine()) != null) {
                    String[] cells = record.trim().split(Delimiter.SPACE);
                    stateCodes.put(cells[1], cells[0]);
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    
            return stateCodes;
        }
    }

}

final class StaffMembersCountsSeeder extends TableSeeder {

    public StaffMembersCountsSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.SPACE);
        PreparedStatement insertion = connection.prepareStatement(Insertion.STAFF_MEMBERS_COUNTS);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setDouble(2, Double.parseDouble(cells[1]));
        insertion.setDouble(3, Double.parseDouble(cells[2]));
        insertion.executeUpdate();
        insertion.close();
    }
}

final class StatesSeeder extends TableSeeder {

    public StatesSeeder(String connectionUrl, String source, String table) {
        super(connectionUrl, source, table);
    }

    @Override
    protected void insert(Connection connection, String record) throws SQLException {
        String[] cells = record.trim().split(Delimiter.SPACE);
        PreparedStatement insertion = connection.prepareStatement(Insertion.STATES);
        insertion.setInt(1, Integer.parseInt(cells[0]));
        insertion.setString(2, cells[1]);
        insertion.executeUpdate();
        insertion.close();
    }
}

final class DbConfig {

    private final String username;
    private final String password;

    public DbConfig(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}

// Util
final class ExecutionTimer {
    
    public static void measure(Runnable task) {
        long start = System.currentTimeMillis();

        task.run();

        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println();
        String result = "Minutes: " + elapsed / 60 + " | " + "Seconds: " + elapsed;
        System.out.println(result);
    }
}

final class SqlServerUtils {

    public static String connectionUrl(String username, String password) {
        return "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
            + "database=cs3380;"
            + "user=" + username + ";"
            + "password="+ password +";"
            + "encrypt=false;"
            + "trustServerCertificate=false;"
            + "loginTimeout=30;";
    }
    
}

final class SqlReader {

    public static String[] read(String file) {
        StringBuffer result = new StringBuffer();
    
        try { 
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = new String();
            while((line = reader.readLine()) != null) {
                result.append((line));
            }
            reader.close();
        } catch(Exception e) {
            e.printStackTrace();
        }

        return result.toString().split(";");
    }
}

final class ConfigReader {

    public static DbConfig read(String file) {

        Properties prop = new Properties();

        try {
            FileInputStream configFile = new FileInputStream(file);
            prop.load(configFile);
            configFile.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Could not find config file.");
            System.exit(1);
        } catch (IOException ex) {
            System.out.println("Error reading config file.");
            System.exit(1);
        }
        
        String username = (prop.getProperty("username"));
        String password = (prop.getProperty("password"));

        if (username == null || password == null){
            System.out.println("Username or password not provided.");
            System.exit(1);
        }

        return new DbConfig(username, password);
    }
}

final class Logger {

    public static void logRunResult(String result) {
        try {
            PrintWriter writer = new PrintWriter(new FileWriter("log/runs.txt"));
            writer.println(result);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// Shared
final class Delimiter {
    
    public static final String 
        SPACE = "\\s++", 
        COMMA = ",", 
        TAB = "\t",
        NEW_LINE = "\n";
}

final class Table {

    public static final String
        T_LIBRARIES = "libraries",
        T_DATABASES_COUNTS = "databases_counts",
        T_STAFF_MEMBERS_COUNTS = "staff_members_counts",
        T_EMPLOYEE_EXPENDITURES = "employee_expenditures",
        T_COLLECTION_EXPENDITURES = "collection_expenditures",
        T_CAPITAL_REVENUES = "capital_revenues",
        T_OPERATING_REVENUES = "operating_revenues",
        T_COUNTIES = "counties",
        T_SCHOOLS = "schools",
        T_STATES = "states";
        
}

final class Insertion {

    public static final String
    
        CAPITAL_REVENUES = 
        "insert into capital_revenues(" +
            "capital_revenue_id," +
            "local_government_capital_revenue," +
            "state_government_capital_revenue," +
            "federal_government_capital_revenue," +
            "other_capital_revenue" +
        ") values(?,?,?,?,?)",

        COLLECTION_EXPENDITURES = 
        "insert into collection_expenditures(" +
            "collection_expenditure_id," +
            "print_collection_expenditures," +
            "digital_collection_expenditures," +
            "other_collection_expenditures" +
        ") values(?,?,?,?)",

        COUNTIES =
        "insert into counties(" +
            "state_code,"+
            "county_code," +
            "county_population," +
            "county_name" +
        ") values(?,?,?,?)",

        DATABASES_COUNTS = 
        "insert into databases_counts(" +
            "databases_count_id," +
            "local_cooperative_agreements," +
            "state_licensed_databases" +
        ") values(?,?,?)",

        EMPLOYEE_EXPENDITURES = 
        "insert into employee_expenditures(" +
            "employee_expenditure_id," +
            "salaries," +
            "benefits" +
        ") values(?,?,?)",

        LIBRARIES = 
        "insert into libraries(" +
            "library_id," +
            "library_name," +
            "street_address," +
            "city," +
            "zipcode," +
            "longitude," +
            "latitude," +
            "state_code," +
            "county_code," +
            "staff_members_count_id," +
            "operating_revenue_id," +
            "employee_expenditure_id," +
            "collection_expenditure_id," +
            "capital_revenue_id," +
            "databases_count_id" +
        ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",

        OPERATING_REVENUES = 
        "insert into operating_revenues(" +
            "operating_revenue_id," +
            "local_government_operating_revenue," +
            "state_government_operating_revenue," +
            "federal_government_operating_revenue," +
            "other_operating_revenue" +
        ") values(?,?,?,?,?)",

        STAFF_MEMBERS_COUNTS = 
        "insert into staff_members_counts(" +
            "staff_members_count_id," +
            "librarians," +
            "employees" +
        ") values(?,?,?)",

        STATES = 
        "insert into states(" +
            "state_code," +
            "state_alpha_code" +
        ") values(?,?)",
        
        SCHOOLS =
        "insert into schools(" +
            "school_code," +
            "school_name," + 
            "state_code" +
        ") values(?,?,?)";
}
