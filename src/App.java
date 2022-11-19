import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public final class App {

    public static void main(String[] args) {
        DbConfig config = ConfigReader.read("auth.cfg");
        // loadDb(config.getUsername(), config.getPassword());
        Analytics.up(config.getUsername(), config.getPassword()).run();
    }

    private static void loadDb(String username, String password) {
        Benchmark.run(() -> DbLoader.up(SqlServerUtils.connectionUrl(username, password)).run());
    }
}

// Queries
final class Analytics {

    private final Connection connection;
    
    private final Map<String, String> cache = new HashMap<>();
    private long cacheLastCleared = System.currentTimeMillis();
    private final Map<String, QueryExecutor> queryExecutors = new HashMap<>();

    private static class Query {

        public static final String

            TEST = "select s.state_code, count(library_id) as count from libraries as l left join states as s on l.state_code = s.state_code group by s.state_code",
            
            TOP_5000_LIBRARIES_BY_ID = "select top 5000 * from libraries where library_id = ? or library_id = ?",

            top_10_counties_ordered_by_libraries_count_then_by_schools_count = 
                "select top 10 " +
                    "counties.county_code, " +
                    "counties.state_code, " +
                    "count(distinct schools.school_code) as schools_count, " +
                    "count(distinct libraries.library_id) as libraries_count " +
                "from counties " +
                "join states on counties.state_code = states.state_code " +
                "join libraries on states.state_code = libraries.state_code and counties.county_code = libraries.county_code " +
                "join schools on states.state_code = schools.state_code " +
                "group by counties.county_code, counties.state_code " +
                "order by libraries_count desc, schools_count desc;";
            
    }

    private static class QueryExecutor {

        private String query;
        private Consumer<String> executing;
        private BiConsumer<String, String[]> executingWithArgs;
        private Set<String> args = new HashSet<>();

        private QueryExecutor(String query, Consumer<String> executing) {
            this.query = query;
            this.executing = executing;
        }

        private QueryExecutor(String query, BiConsumer<String, String[]> executingWithArgs, String...args) {
            this.query = query;
            this.executingWithArgs = executingWithArgs;
            Collections.addAll(this.args, args);
        }

        private static QueryExecutor use(String query, Consumer<String> executing) {
            return new QueryExecutor(query, executing);
        }

        private static QueryExecutor use(String query, BiConsumer<String, String[]> executingWithArgs, String...args) {
            return new QueryExecutor(query, executingWithArgs, args);
        }

        public void run() {
            if (args.size() > 0) {
                runQueryWithArgs();
            } else {
                runQuery();
            }
        }

        private void runQuery() {
            Benchmark.run(() -> executing.accept(query));
        }

        private void runQueryWithArgs() {
            displayQueryArgs();
            Scanner scanner = new Scanner(System.in);
            String line = scanner.nextLine();
            String[] inputs = line.trim().split(Delimiter.TAB);
        
            int retry = 3;
            do {
        
                if (this.args.size() == inputs.length) {
                    Benchmark.run(params -> executingWithArgs.accept(query, params), inputs);
                    break;
                }
        
                retry--;
                displayRetryMessage(retry);
                
                if (retry > 0) {
                    displayQueryArgs();
                    line = scanner.nextLine();
                    inputs = line.trim().split(Delimiter.TAB);
                }

            } while (retry > 0);

            scanner.close();
        }

        private void displayRetryMessage(int retry) {
            System.out.println("\nThe args count is not valid");
            System.out.println("\nYou have " + retry + " times left for retry");
        }

        public void displayQueryArgs() {
            System.out.println("\nYou have selected a query with args");
            System.out.println("\nArgs count: " + args.size());
            System.out.println("\nPlease enter a value for each of the args separated by TAB");
            args.forEach(arg -> System.out.print(arg + " "));
            System.out.println();
            System.out.println();
        }
    }

    private Analytics(Connection connection) {
        this.connection = connection;
        registerQueryExecutors();
    }

    private void registerQueryExecutors() {
        queryExecutors.put("1", QueryExecutor.use(Query.TEST, this::query1));
        queryExecutors.put("2", QueryExecutor.use(Query.TOP_5000_LIBRARIES_BY_ID, this::query2, "library_id1", "library_id2"));
        queryExecutors.put("3", QueryExecutor.use(Query.top_10_counties_ordered_by_libraries_count_then_by_schools_count, this::query3));

    }

    public static Analytics up(String username, String password) {
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(SqlServerUtils.connectionUrl(username, password));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new Analytics(connection);
    }

    public void run() {

        displayReportsDirectory();

        Scanner scanner = new Scanner(System.in);
        String line = scanner.nextLine();
        while (line != null && !line.equals("q")) {
            String[] inputs = line.trim().split(Delimiter.SPACE);

            if (inputs.length == 0) {
                displayReportsDirectory();
                line = scanner.nextLine();
                continue;
            }

            queryExecutors
                .getOrDefault(inputs[0], QueryExecutor.use("\nThe option is not on our directory", System.out::println))
                .run();

            displayReportsDirectory();
            line = scanner.nextLine();
        }

        scanner.close();
    }

    private void query2(String query, String[] args) {
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, args[0]);
            statement.setString(2, args[1]);
            ResultSet resultSet = statement.executeQuery();
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                String result = "| " + resultSet.getString("library_name") + " |";
                results.add(result);
            }
            
            if (results.isEmpty()) {
                displayNotFound();
            } else {
                System.out.println("| Libary |");
                System.out.println(joinResults(results));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void query3(String query) {

        if (applyCacheIfPresent("3")) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                String result = "| " + 
                    resultSet.getInt("county_code") + " | " +
                    resultSet.getInt("state_code") + " | " +
                    resultSet.getInt("schools_count") + " | " +
                    resultSet.getInt("libraries_count") + " |";
                results.add(result);
            }

            if (results.isEmpty()) {
                displayNotFound();
            } else {
                String table = joinResults(results);
                System.out.println(table);
                cache.put("3", table);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void query1(String query) {

        if (applyCacheIfPresent("1")) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                String result = "| " + 
                    resultSet.getString("state_code") + " | " +
                    resultSet.getInt("count") + " |";
                results.add(result);
            }

            if (results.isEmpty()) {
                displayNotFound();
            } else {
                System.out.println("| Libary |");
                String table = joinResults(results);
                System.out.println(table);
                cache.put("1", table);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String joinResults(List<String> results) {
        return results.stream().collect(Collectors.joining(Delimiter.NEW_LINE));
    }

    private boolean applyCacheIfPresent(String key) {
        ensureCacheRefreshed();
        String data = cache.getOrDefault(key, null);
        boolean result = data != null;
        if (result) System.out.println(data);
        return result;
    }

    private void ensureCacheRefreshed() {
        double elapsedTimeSinceLastCacheClearedInMinutes = ((System.currentTimeMillis() - cacheLastCleared) / 1000.0) / 60;
        if (elapsedTimeSinceLastCacheClearedInMinutes > 0.2) {
            cache.clear();
            cacheLastCleared = System.currentTimeMillis();
        }
    }

    private static void displayReportsDirectory() {
        System.out.println("\n         Reports Directory");
        System.out.println("----------------------------------------");
        System.out.println("1 - Q 1");
        System.out.println("2 - Q 2");
        System.out.println("3 - Q 3");
        System.out.println("q - End\n");
    }

    private void displayNotFound() {
        System.out.println("\n\n - - - No records are found - - -\n\n");
    }
}

// Data
final class DbLoader {

    private String connectionUrl;
    private TableSeedersExecutor tableSeedersExecutor;
    
    private DbLoader(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        this.tableSeedersExecutor = new TableSeedersExecutor(connectionUrl);
    }

    public static DbLoader up(String connectionUrl) {
        return new DbLoader(connectionUrl);
    }

    public void run() {
        createTablesIfAbsent();
        tableSeedersExecutor.run();
    }

    private void createTablesIfAbsent() {
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

    protected final String connectionUrl, file, table;

    public TableSeeder(String connectionUrl, String file, String table) {
        this.connectionUrl = connectionUrl;
        this.file = file;
        this.table = table;
    }

    public void seed() {
        try {
            Connection connection = DriverManager.getConnection(connectionUrl);

            if (isTableSeeded(connection, table)) {
                return;
            }

            runBatch(connection);
            connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected abstract void runBatch(Connection connection) throws SQLException, IOException;

    protected boolean isTableSeeded(Connection connection, String table) throws SQLException {
        Statement selection = connection.createStatement();
        ResultSet result = selection.executeQuery("select * from " + table);
        return result.next();
    }
}

final class CapitalRevenuesSeeder extends TableSeeder {

    public CapitalRevenuesSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.CAPITAL_REVENUES);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.SPACE);
            insertion.setInt(1, Integer.parseInt(cells[0]));
            insertion.setDouble(2, Double.parseDouble(cells[1]));
            insertion.setDouble(3, Double.parseDouble(cells[2]));
            insertion.setDouble(4, Double.parseDouble(cells[3]));
            insertion.setDouble(5, Double.parseDouble(cells[4]));
            insertion.addBatch();
        }

        insertion.executeBatch();
        insertion.close();
        reader.close();
    } 
}

final class CollectionExpendituresSeeder extends TableSeeder {

    public CollectionExpendituresSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.COLLECTION_EXPENDITURES);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.SPACE);
            insertion.setInt(1, Integer.parseInt(cells[0]));
            insertion.setDouble(2, Double.parseDouble(cells[1]));
            insertion.setDouble(3, Double.parseDouble(cells[2]));
            insertion.setDouble(4, Double.parseDouble(cells[3]));
            insertion.addBatch();
        }
        
        insertion.executeBatch();
        insertion.close();
        reader.close();
    }
}

final class CountiesSeeder extends TableSeeder {

    public CountiesSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.COUNTIES);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.COMMA);
            insertion.setInt(1, Integer.parseInt(cells[0]));
            insertion.setInt(2, Integer.parseInt(cells[1]));
            insertion.setInt(3, Integer.parseInt(cells[2]));
            insertion.setString(4, cells[3]);
            insertion.addBatch();
        }

        insertion.executeBatch();
        insertion.close();
        reader.close();
    }
}

final class DatabasesCountsSeeder extends TableSeeder {

    public DatabasesCountsSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.DATABASES_COUNTS);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.SPACE);
            insertion.setInt(1, Integer.parseInt(cells[0]));
            insertion.setInt(2, Integer.parseInt(cells[1]));
            insertion.setInt(3, Integer.parseInt(cells[2]));
            insertion.addBatch();
        }

        insertion.executeBatch();
        insertion.close();
        reader.close();
    }
}

final class EmployeeExpendituresSeeder extends TableSeeder {

    public EmployeeExpendituresSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.EMPLOYEE_EXPENDITURES);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.SPACE);
            insertion.setInt(1, Integer.parseInt(cells[0]));

            if (cells.length > 1) {
                insertion.setDouble(2, Double.parseDouble(cells[1]));
                insertion.setDouble(3, Double.parseDouble(cells[2]));
            } else {
                insertion.setDouble(2, 0);
                insertion.setDouble(3, 0);
            }
            insertion.addBatch();
        }

        insertion.executeBatch();
        insertion.close();
        reader.close();
    }
}

final class LibrariesSeeder extends TableSeeder {

    public LibrariesSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.LIBRARIES);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.TAB);
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
            insertion.addBatch();
        }

        insertion.executeBatch();
        insertion.close();
        reader.close();
    }
}

final class OperatingRevenuesSeeder extends TableSeeder {

    public OperatingRevenuesSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.OPERATING_REVENUES);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.SPACE);
            insertion.setInt(1, Integer.parseInt(cells[0]));
            insertion.setDouble(2, Double.parseDouble(cells[1]));
            insertion.setDouble(3, Double.parseDouble(cells[2]));
            insertion.setDouble(4, Double.parseDouble(cells[3]));
            insertion.setDouble(5, Double.parseDouble(cells[4]));
            insertion.addBatch();
        }

        insertion.executeBatch();
        insertion.close();
        reader.close();
    }
}

final class SchoolsSeeder extends TableSeeder {

    public SchoolsSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }


    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.SCHOOLS);

        new SchoolsReader(file).read().forEach(record -> {
            try {
                String[] cells = record.trim().split(Delimiter.COMMA);
                insertion.setInt(1, Integer.parseInt(cells[0]));
                insertion.setString(2, cells[1]);
                insertion.setInt(3, Integer.parseInt(cells[2]));
                insertion.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        insertion.executeBatch();
        insertion.close(); 
    }

    private static class SchoolsReader {
        private final String file;

        public SchoolsReader(String file) {
            this.file = file;
        }

        public List<String> read() {
            List<String> schools = new ArrayList<>();
            Map<String, String> states = readStates(); // // NAME -> AA
            Map<String, String> stateCodes = readStateCodes(); // AA - 1
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
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

    public StaffMembersCountsSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.STAFF_MEMBERS_COUNTS);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.SPACE);
            insertion.setInt(1, Integer.parseInt(cells[0]));
            insertion.setDouble(2, Double.parseDouble(cells[1]));
            insertion.setDouble(3, Double.parseDouble(cells[2]));
            insertion.addBatch();
        }
        
        insertion.executeBatch();
        insertion.close();
        reader.close();
    }
}

final class StatesSeeder extends TableSeeder {

    public StatesSeeder(String connectionUrl, String file, String table) {
        super(connectionUrl, file, table);
    }

    @Override
    protected void runBatch(Connection connection) throws SQLException, IOException {
        PreparedStatement insertion = connection.prepareStatement(Insertion.STATES);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.readLine();

        String record = "";
        while ((record = reader.readLine()) != null) {
            String[] cells = record.trim().split(Delimiter.SPACE);
            insertion.setInt(1, Integer.parseInt(cells[0]));
            insertion.setString(2, cells[1]);
            insertion.addBatch();
        }
        
        insertion.executeBatch();
        insertion.close();
        reader.close();
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
final class Benchmark {
    
    public static <T> void run(Consumer<T> consumer, T element) {
        long start = System.currentTimeMillis();
        consumer.accept(element);
        displayMetrics(start);
    }

    public static <T> void run(Runnable action) {
        long start = System.currentTimeMillis();
        action.run();
        displayMetrics(start);
    }

    private static void displayMetrics(long start) {
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println();
        String result = "Minutes: " + elapsed / 60 + " | " + "Seconds: " + elapsed;
        System.out.println(result);
        System.out.println();
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
