import java.io.*;
import java.util.*;
import javax.swing.*;
import java.util.function.*;
import java.sql.*;
import java.awt.Container;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.*;
import java.awt.Color;

public final class App {

    public static void main(String[] args) {

        DbConfig config = ConfigReader.read("auth.cfg");
        JFrame frame = UI.createCenterFrame("US Libraries Analyzer");

        Runnable seeder = () -> seedDatabase(config.getUsername(), config.getPassword(), () -> {
            frame.dispose();
            System.out.println("The database is seeded succesfully\n");
            System.out.println("Please make a selection");
        });

        JButton button = UI.createButton(
            "Seed the database", 
            seeder);

        frame.add(button);

        UsLibrariesAnalyzer
            .connectToDatabase(config.getUsername(), config.getPassword())
            .run(() -> frame.dispose(), seeder);
    }

    private static void seedDatabase(String username, String password, Runnable callback) {
        Benchmark.run(() -> 
            DatabaseLoader
                .up(SqlServerUtils.connectionUrl(username, password))
                .run());
        callback.run();
    }
}

// UI
final class UI extends JFrame {

    public static JFrame createCenterFrame(String title) {
        JFrame frame = new JFrame();
        frame.setTitle(title);
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.setBounds(100, 50, 900, 300);
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
        frame.toFront();
        return frame;
    }

    public static JFrame createFrame(String title) {
        JFrame frame = new JFrame();
        frame.setTitle(title);
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.setBounds(100, 50, 900, 300);
        frame.setVisible(true);
        frame.toFront();
        return frame;
    }

    public static JButton createButton(String text, Runnable action) {
        JButton button = new JButton(text);
        button.setBounds(300, 100, 250, 25);
        button.addActionListener(e -> {
            button.setEnabled(false);
            action.run();
            button.setEnabled(true);
        });
        return button;
    }

    public static JLabel createLabel(String text) {
        JLabel label = new JLabel("query", JLabel.CENTER);
        label.setAlignmentX(0);
        label.setAlignmentY(0);
        label.setText(text);
        label.setVisible(true);
        return label;
    }

    public static void tabulate(String[][] rows, String[] header, String title) {
        JFrame frame = new JFrame();
        frame.setTitle(title);
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        Container container = frame.getContentPane();
        JTable table = new JTable(rows, header);
        JScrollPane pane = new JScrollPane(table);
        container.add(pane, BorderLayout.CENTER);
        frame.setSize(1000, 500);
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }
}

// Queries
final class UsLibrariesAnalyzer {

    private final Map<String, CacheRecord> cache = new HashMap<>();
    private long cacheLastCleared = System.currentTimeMillis();

    private static class CacheRecord {
        final String title;
        final String[] header;
        final String[][] records;

        public CacheRecord(String title, String[] header, String[][] records) {
            this.title = title;
            this.header = header;
            this.records = records;
        }
    }

    private final Connection connection;
    private final Map<Integer, QueryExecutor> queryExecutors = new HashMap<>();

    public static UsLibrariesAnalyzer connectToDatabase(String username, String password) {
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(SqlServerUtils.connectionUrl(username, password));
        } catch (SQLException e) { }

        return new UsLibrariesAnalyzer(connection);
    }

    public void run(Runnable frameDisposer, Runnable databaseSeeder) {
        if (connection == null) {
            System.out.println("Failed to load the database");
            return;
        }

        displayReportsDirectory();

        Scanner scanner = new Scanner(System.in);
        String line = scanner.nextLine();
        frameDisposer.run();

        if (!isDatabaseSeeded(connection)) {
            databaseSeeder.run();
        }
        
        while (line != null && !line.equals("q")) {
            String[] inputs = line.trim().split(Delimiter.SPACE);

            if (inputs.length == 0) {
                displayReportsDirectory();
                line = scanner.nextLine();
                continue;
            }

            String input = inputs[0];
            if (!NumbersUtil.isInteger(input)) {
                System.out.println("--- Please enter a number ---");
                displayReportsDirectory();
                line = scanner.nextLine();
                continue;
            }

            int key = Integer.parseInt(inputs[0]);
            queryExecutors
                .getOrDefault(key, queryExecutors.get(0))
                .run(() -> displayReportsDirectory());
            line = scanner.nextLine();
        }

        JOptionPane.showMessageDialog(null, "Thank you for using our services");
        scanner.close();
    }

    private static class SqlQuery {

        public static final String
            
            libraries_with_id_of_id1_or_id2 = 
                "select library_id, library_name from libraries where (libraries.library_id = ? ) or (libraries.library_id = ? )",

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
                "order by libraries_count desc, schools_count desc;",

            libraries_ordered_by_total_operating_revenue = 
                "select " +
                    "library_name, " +
                    "(local_government_operating_revenue + state_government_operating_revenue + federal_government_operating_revenue + other_operating_revenue) as total_operating_revenue " +
                "from libraries " +
                "join operating_revenues on libraries.operating_revenue_id = operating_revenues.operating_revenue_id " +
                "order by total_operating_revenue",
            
            average_state_licensed_databases_per_library_for_counties_that_belong_to_states_with_less_than_5_counties = 
                "select " +
                    "outer_counties.county_code, " +
                    "outer_counties.state_code, " +
                    "sum(databases_counts.state_licensed_databases) / count(libraries.library_id) average_state_licensed_databases_per_library_for_county " +
                "from counties as outer_counties " +
                "join states on outer_counties.state_code = states.state_code " +
                "join libraries on states.state_code = libraries.state_code and outer_counties.county_code = libraries.county_code " +
                "join databases_counts on libraries.databases_count_id = databases_counts.databases_count_id " +
                "where outer_counties.state_code in ( " +
                    "select states.state_code " +
                    "from states " +
                    "join counties on states.state_code = counties.state_code " +
                    "where states.state_code = outer_counties.state_code " +
                    "group by states.state_code " +
                    "having count(counties.county_code) < 5 " +
                ") " +
                "group by outer_counties.county_code, outer_counties.state_code " +
                "order by average_state_licensed_databases_per_library_for_county",

            library_count_per_county =
                "select " +
                    "cast( state_alpha_code as varchar(8000) ) as state_alpha_code, " +
                    "cast( county_name as varchar(8000) ) as county_name, " +
                    "county_population, " +
                    "count( libraries.library_id ) as library_count " +
                "from counties " +
                "join libraries on counties.county_code = libraries.county_code " +
                "join states on counties.state_code = states.state_code " +
                "group by " +
                    "cast( state_alpha_code as varchar(8000) ), " +
                    "county_population, " +
                    "cast( county_name as varchar(8000) ) " +
                "order by library_count desc",

            database_count_per_library =
                "select " +
                    "library_name, " +
                    "local_cooperative_agreements, " +
                    "state_licensed_databases, "  +
                    "(local_cooperative_agreements + state_licensed_databases ) as total_databases " +
                "from libraries " +
                "join databases_counts on libraries.databases_count_id = databases_counts.databases_count_id " +
                "order by total_databases desc",

            addresses_of_each_library =
                "select " +
                    "library_name, " +
                    "street_address, " +
                    "city, " +
                    "zipcode, " +
                    "state_alpha_code, " +
                    "county_name, " +
                    "latitude, " +
                    "longitude " +
                "from libraries " +
                "join states on libraries.state_code = states.state_code " +
                "join counties on libraries.state_code = counties.state_code and libraries.county_code = counties.county_code",
            
            top_10_most_expensive_libraries_to_run =
                "select top 10 " +
                    "library_name, " +
                    "( " +
                        "(local_government_operating_revenue + state_government_operating_revenue + federal_government_operating_revenue +  other_operating_revenue " +
                        "+ local_government_capital_revenue + state_government_capital_revenue + federal_government_capital_revenue + other_capital_revenue) " +
                        "- " +
                        "(salaries + benefits " +
                        "+ print_collection_expenditures + digital_collection_expenditures + other_collection_expenditures) " +
                        
                    ") as total_cost " +
                "from libraries " +
                "join operating_revenues on libraries.operating_revenue_id = operating_revenues.operating_revenue_id " +
                "join capital_revenues on libraries.capital_revenue_id = capital_revenues.capital_revenue_id " +
                "join collection_expenditures on libraries.collection_expenditure_id = collection_expenditures.collection_expenditure_id " +
                "join employee_expenditures on libraries.employee_expenditure_id = employee_expenditures.employee_expenditure_id " +
                "order by total_cost desc",

            top_10_libraries_with_highest_average_pay =
                "select top 10" +
                "library_name, " +
                "( (salaries + benefits) / nullif((librarians + employees), 0) ) as average_pay " +
                "from libraries " +
                "join staff_members_counts on libraries.staff_members_count_id = staff_members_counts.staff_members_count_id " +
                "join employee_expenditures on libraries.employee_expenditure_id = employee_expenditures.employee_expenditure_id " +
                "order by average_pay desc ",

            staff_count_and_staff_pay_per_library =
                "select " +
                    "library_name, " +
                    "librarians, " +
                    "employees, " +
                    "(librarians + employees) as total_staff, " +
                    "salaries, " +
                    "benefits, " +
                    "(salaries + benefits) as total_employee_expenditures " +
                "from libraries " +
                "join staff_members_counts on libraries.staff_members_count_id = staff_members_counts.staff_members_count_id " +
                "join employee_expenditures on libraries.employee_expenditure_id = employee_expenditures.employee_expenditure_id " +
                "order by total_staff desc, total_employee_expenditures desc",

            capital_revenues_of_each_library_ordered_most_to_least =
                "select " +
                    "library_name, " +
                    "local_government_capital_revenue, " +
                    "state_government_capital_revenue, " +
                    "federal_government_capital_revenue, " +
                    "other_capital_revenue, " +
                    "(local_government_capital_revenue + state_government_capital_revenue + federal_government_capital_revenue + other_capital_revenue) as total_capital_revenue " +
                "from libraries " +
                "join capital_revenues on libraries.capital_revenue_id = capital_revenues.capital_revenue_id " +
                "order by total_capital_revenue desc",

            operating_revenues_of_each_library_ordered_most_to_least =
                "select " +
                    "library_name, " +
                    "local_government_operating_revenue, " +
                    "state_government_operating_revenue, " +
                    "federal_government_operating_revenue, " +
                    "other_operating_revenue, " +
                    "(local_government_operating_revenue + state_government_operating_revenue + federal_government_operating_revenue + other_operating_revenue) as total_operating_revenue " +
                "from libraries " +
                "join operating_revenues on libraries.operating_revenue_id = operating_revenues.operating_revenue_id " +
                "order by total_operating_revenue desc",

            collection_expenditures_of_each_library_ordered_most_to_least =
                "select " +
                    "library_name, " +
                    "print_collection_expenditures, " +
                    "digital_collection_expenditures, " +
                    "other_collection_expenditures, " +
                    "(print_collection_expenditures + digital_collection_expenditures + other_collection_expenditures) as total_collection_expenditures " +
                "from libraries " +
                "join collection_expenditures on libraries.collection_expenditure_id = collection_expenditures.collection_expenditure_id " +
                "order by total_collection_expenditures desc",

            
            schools_with_state_population =
                "with state_pop as ( " +
                    "select " +
                        "counties.state_code, " +
                        "sum(county_population) as state_population " +
                    "from counties " +
                    "join states on counties.state_code = states.state_code " + 
                    "group by counties.state_code " +
                ") " +
                "select " +
                    "schools.school_name, " +
                    "states.state_alpha_code, " +
                    "state_pop.state_population " +
                "from schools " +
                "join states on schools.state_code = states.state_code " +
                "join state_pop on schools.state_code = state_pop.state_code";
    }

    private static final class Query {
        private static int index = 0;

        private int key;
        private String header;
        private String body;

        public Query() {
            this.key = index;
            index++;
        }

        public int key() {
            return this.key;
        }

        public void header(String header) {
            this.header = header;
        }

        public void body(String body) {
            this.body = body;
        }

        @Override
        public String toString() {
            return body;
        }
    }

    private static final class QueryExecutor {

        private static class Builder {
            private Query query;
            private Consumer<Query> execution;
            private BiConsumer<Query, String[]> executionWithArgs;
            private Set<String> args = new HashSet<>();

            public Builder() {
                query = new Query();
            }

            public Builder header(String header) {
                query.header(header);
                return this;
            }

            public Builder body(String body) {
                query.body(body);
                return this;
            }

            public Builder toExecute(Consumer<Query> execution) {
                this.execution = execution;
                return this;
            }

            public Builder toExecute(BiConsumer<Query, String[]> executionWithArgs) {
                this.executionWithArgs = executionWithArgs;
                return this;
            }

            public Builder args(String... args) {
                Collections.addAll(this.args, args);
                return this;
            }

            public QueryExecutor build() {
                return new QueryExecutor(this);
            }
        }

        private Query query;
        private Consumer<Query> execution;
        private BiConsumer<Query, String[]> executionWithArgs;
        private Set<String> args = new HashSet<>();

        private QueryExecutor(Builder builder) {
            this.query = builder.query;
            this.execution = builder.execution;
            this.executionWithArgs = builder.executionWithArgs;
            this.args = builder.args;
        }

        @Override
        public String toString() {
            return key() + " " + this.query.header;
        }

        public int key() {
            return this.query.key();
        }

        private static Builder builder() {
            return new Builder();
        }

        public void run(Runnable callback) {
            if (args.size() > 0) runQueryWithArgs(callback);
            else runQuery(callback);
        }

        private void runQuery(Runnable callback) {
            Benchmark.run(() -> execution.accept(query));
            callback.run();
        }

        // Do NOT touch !
        // It works !
        private void runQueryWithArgs(Runnable callback) {
            String[] queryArgs = args.toArray(String[]::new);
            JTextField[] fields = new JTextField[args.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = new JTextField(15);
            }            
            
            JButton submit = new JButton("Submit");

            JFrame frame = new JFrame();
            frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
            frame.setSize(800, 500);
            frame.setTitle(query.header + " ( " + args.size() + " parameters)");

            ActionListener listener = e -> {
                Object o = e.getSource();
                if (o == submit) {
                    String[] inputs = new String[args.size()];
                    for (int i = 0; i < inputs.length; i++) {
                        inputs[i] = fields[i].getText();
                    }

                    Benchmark.run(params -> executionWithArgs.accept(query, params), inputs);
                    callback.run();
                    frame.dispose();
                }
            };

            submit.addActionListener(listener);

            Container container = frame.getContentPane();
            container.setLayout(new FlowLayout());
            
            
            for (int i = 0; i < fields.length; i++) {
                JTextField field = fields[i];
                String arg = queryArgs[i];

                field.addMouseListener(new MouseListener() {

                    @Override
                    public void mouseClicked(MouseEvent e) {
                    }

                    @Override
                    public void mousePressed(MouseEvent e) {
                        if (field.getText().equals("")) {
                            field.setText(arg);
                            field.setForeground(Color.GRAY);
                        }
                    }

                    @Override
                    public void mouseReleased(MouseEvent e) {
                        if (field.getText().equals(arg)) {
                            try {
                                // I know this line sucks
                                Thread.sleep(500);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                            field.setText("");
                            field.setForeground(Color.BLACK);
                        }
                    }

                    @Override
                    public void mouseEntered(MouseEvent e) {
                    }

                    @Override
                    public void mouseExited(MouseEvent e) {
                    }

                });

                container.add(field);
            }

            container.add(submit);
            frame.setVisible(true);
        }
    }

    private UsLibrariesAnalyzer(Connection connection) {
        if (connection == null) {
            throw new RuntimeException("Failed to connect to database");
        }

        this.connection = connection;
        registerQueryExecutors();
    }

    private void registerQueryExecutors() {

        QueryExecutor executor = QueryExecutor.builder()
            .header("Default")
            .body("Option not found")
            .toExecute(System.out::println)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Library with total operating revenue closest to n")
            .body(SqlQuery.libraries_ordered_by_total_operating_revenue)
            .toExecute(this::librariesOrderedByTotalOperatingRevenue)
            .args("n")
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Libraries with ID of library_ID_1 or library_ID_2")
            .body(SqlQuery.libraries_with_id_of_id1_or_id2)
            .toExecute(this::LibrariesWithIdOfId1OrId2)
            .args("library_ID_1 (e.g. AK0001)", "library_ID_2 (e.g. WY0023)")
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Average state licensed databases per library for counties that belong to states with less than 5 counties")
            .body(SqlQuery.average_state_licensed_databases_per_library_for_counties_that_belong_to_states_with_less_than_5_counties)
            .toExecute(this::averageStateLicensedDatabasesPerLibraryForCountiesThatBelongToStatesWithLessThan5Counties)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Top 10 libraries with the highest average pay per employee")
            .body(SqlQuery.top_10_libraries_with_highest_average_pay)
            .toExecute(this::Top10LibrariesWithHighestAveragePayPerEmployee)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Schools with their state's total population")
            .body(SqlQuery.schools_with_state_population)
            .toExecute(this::schoolsWithStateTotalPopulation)
            .build();
        queryExecutors.put(executor.key(), executor);    

        executor = QueryExecutor.builder()
            .header("Top 10 counties ordered by libraries count then by schools count")
            .body(SqlQuery.top_10_counties_ordered_by_libraries_count_then_by_schools_count)
            .toExecute(this::top10CountiesOrderedByLibrariesCountThenBySchoolsCount)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Library count per county")
            .body(SqlQuery.library_count_per_county)
            .toExecute(this::librariesCountForEachCounty)
            .build();
        queryExecutors.put(executor.key(), executor);
        
        executor = QueryExecutor.builder()
            .header("Database count per library")
            .body(SqlQuery.database_count_per_library)
            .toExecute(this::databasesCountForEachLibrary)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Address of each library")
            .body(SqlQuery.addresses_of_each_library)
            .toExecute(this::addressForEachLibrary)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Top 10 most expensive libraries to run")
            .body(SqlQuery.top_10_most_expensive_libraries_to_run)
            .toExecute(this::top10MostExpensiveLibrariesToRun)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Staff count and staff pay per library")
            .body(SqlQuery.staff_count_and_staff_pay_per_library)
            .toExecute(this::staffCountAndStaffPayPerLibrary)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Capital revenues of each library desc")
            .body(SqlQuery.capital_revenues_of_each_library_ordered_most_to_least)
            .toExecute(this::capitalRevenuesForEachLibraryDesc)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Operating revenues of each library desc")
            .body(SqlQuery.operating_revenues_of_each_library_ordered_most_to_least)
            .toExecute(this::operatingRevenuesForEachLibraryDesc)
            .build();
        queryExecutors.put(executor.key(), executor);

        executor = QueryExecutor.builder()
            .header("Collection expenditures of each library desc")
            .body(SqlQuery.collection_expenditures_of_each_library_ordered_most_to_least)
            .toExecute(this::collectionExpendituresForEachLibraryDesc)
            .build();
        queryExecutors.put(executor.key(), executor); 
    }

    private String[][] toTableRecords(List<String[]> results) {
        String[][] records = new String[results.size()][results.get(0).length];
        for (int i = 0; i < records.length; i++) {
            records[i] = results.get(i);
        }
        return records;
    }

    private boolean applyCacheIfPresent(String key) {
        ensureCacheRefreshed();
        CacheRecord cached = cache.getOrDefault(key, null);
        boolean result = cached != null;
        if (result) UI.tabulate(cached.records, cached.header, "Cached: " + cached.title);
        return result;
    }

    private void ensureCacheRefreshed() {
        double elapsedTimeSinceLastCacheClearedInMinutes = ((System.currentTimeMillis() - cacheLastCleared) / 1000.0) / 60;
        if (elapsedTimeSinceLastCacheClearedInMinutes > 0.5) {
            cache.clear();
            cacheLastCleared = System.currentTimeMillis();
        }
    }

    private void displayReportsDirectory() {
        System.out.println("\n         Reports Directory");
        System.out.println("----------------------------------------");
        
        for (int i = 1; i < queryExecutors.values().size(); i++) {
            System.out.println(queryExecutors.get(i));
        }

        System.out.println("q - End\n");
        System.out.println("Please make a selection");
    }

    private void displayNotFound() {
        JOptionPane.showMessageDialog(null, "- - - No records are found - - -");
        System.out.println("\n\n - - - No records are found - - -\n\n");
    }

    private final static class Library {
        final String name;
        final double totalOperatingRevenue;

        public Library(String name, double totalOperatingRevenue) {
            this.name = name;
            this.totalOperatingRevenue = totalOperatingRevenue;
        }
    }

    private boolean isDatabaseSeeded(Connection connection) {
        boolean result = false;

        try {
            ResultSet resultSet = connection
                .getMetaData()
                .getTables(null, null, "libraries", null);
            result = resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    private void librariesOrderedByTotalOperatingRevenue(Query query, String[] args) {
        if (!NumbersUtil.isDouble(args[0])) {
            System.out.println("\n--- The input must be numerical ---");
            return;
        }

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<Library> libraries = new ArrayList<>();
            while (resultSet.next()) {
                libraries.add(
                    new Library(
                        resultSet.getString("library_name"), 
                        resultSet.getDouble("total_operating_revenue")
                    )
                );
            }

            double n = Double.parseDouble(args[0]);
            int i = 0, j = libraries.size() - 1;
            while (i < j - 1) {
                int k = i + (j - i) / 2;
                
                if (libraries.get(k).totalOperatingRevenue < n) {
                    i = k;
                } else {
                    j = k;
                }
            }

            Library library = null, left = libraries.get(i), right = libraries.get(j);
            if (n < left.totalOperatingRevenue) {
                library = left;
            } else if (n > right.totalOperatingRevenue) {
                library = right;
            } else {
                library = n - left.totalOperatingRevenue < right.totalOperatingRevenue - n
                    ? left
                    : right;
            }

            String[][] record = {{ library.name, String.valueOf(library.totalOperatingRevenue), String.valueOf(n) }};
            String[] header = { "Library", "Total Operating Revenue", "n"};
            UI.tabulate(record, header, query.header);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void LibrariesWithIdOfId1OrId2(Query query, String[] args) {
        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            statement.setString(1, args[0]);
            statement.setString(2, args[1]);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_id"),
                    resultSet.getString("library_name"),
                } ;
                results.add(result);
            }
            
            if (results.isEmpty()) displayNotFound();
            else {              
                String[][] records = toTableRecords(results);
                String[] header = { "Library ID", "Library" };
                UI.tabulate(records, header, query.header);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void averageStateLicensedDatabasesPerLibraryForCountiesThatBelongToStatesWithLessThan5Counties(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    String.valueOf(resultSet.getInt("county_code")),
                    String.valueOf(resultSet.getInt("state_code")),
                    String.valueOf(resultSet.getInt("average_state_licensed_databases_per_library_for_county"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "County Code", "State Code", "Average State Licensed Databases per Library for County" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void top10CountiesOrderedByLibrariesCountThenBySchoolsCount(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    String.valueOf(resultSet.getInt("county_code")),
                    String.valueOf(resultSet.getInt("state_code")),
                    String.valueOf(resultSet.getInt("libraries_count")),
                    String.valueOf(resultSet.getInt("schools_count")),
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "County Code", "State Code", "Libraries", "Schools" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void librariesCountForEachCounty(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("county_name"),
                    resultSet.getString("county_population"),
                    resultSet.getString("state_alpha_code"),
                    String.valueOf(resultSet.getInt("library_count"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                System.out.println("County name | County population | State alpha code | Library count |");
                String[][] records = toTableRecords(results);
                String[] header = { "County", "County Population", "State Alpha Code", "Libraries" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void databasesCountForEachLibrary(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = { 
                    resultSet.getString("library_name"),
                    String.valueOf(resultSet.getInt("local_cooperative_agreements")),
                    String.valueOf(resultSet.getInt("state_licensed_databases")),
                    String.valueOf(resultSet.getInt("total_databases")),
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Local Cooperative Agreements", "State Licensed Databases", "Total Databases" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addressForEachLibrary(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_name"),
                    resultSet.getString("street_address"),
                    resultSet.getString("city"),
                    resultSet.getString("zipcode"),
                    resultSet.getString("state_alpha_code"),
                    resultSet.getString("county_name"),
                    String.valueOf(resultSet.getInt("latitude")),
                    String.valueOf(resultSet.getInt("longitude"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Street", "City", "Zipcode", "State Alpha Code", "County", "Latitude", "Longitude" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void top10MostExpensiveLibrariesToRun(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_name"),
                    String.valueOf(resultSet.getDouble("total_cost"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Total Cost" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void Top10LibrariesWithHighestAveragePayPerEmployee(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_name"),
                    String.valueOf(resultSet.getInt("average_pay"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Average Pay" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void staffCountAndStaffPayPerLibrary(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_name"),
                    String.valueOf(resultSet.getInt("librarians")),
                    String.valueOf(resultSet.getInt("employees")),
                    String.valueOf(resultSet.getInt("total_staff")),
                    String.valueOf(resultSet.getInt("salaries")),
                    String.valueOf(resultSet.getInt("benefits")),
                    String.valueOf(resultSet.getInt("total_employee_expenditures"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Librarians", "Employees", "Total Staff", "Salaries", "Benefits", "Total Employee Expenditures" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void capitalRevenuesForEachLibraryDesc(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_name"),
                    String.valueOf(resultSet.getInt("local_government_capital_revenue")),
                    String.valueOf(resultSet.getInt("state_government_capital_revenue")),
                    String.valueOf(resultSet.getInt("federal_government_capital_revenue")),
                    String.valueOf(resultSet.getInt("other_capital_revenue")),
                    String.valueOf(resultSet.getInt("total_capital_revenue"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Local Government Operating Revenue", "State Government Operating Revenue", "Federal Government Operating Revenue", "Other Capital Revenue", "Federal Government Operating Revenue" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void operatingRevenuesForEachLibraryDesc(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_name"),
                    String.valueOf(resultSet.getInt("local_government_operating_revenue")),
                    String.valueOf(resultSet.getInt("state_government_operating_revenue")),
                    String.valueOf(resultSet.getInt("federal_government_operating_revenue")),
                    String.valueOf(resultSet.getInt("other_operating_revenue")),
                    String.valueOf(resultSet.getInt("total_operating_revenue")),
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Local Government Operating Revenue", "State Government Operating Revenue", "Federal Government Operating Revenue", "Other Capital Revenue", "Federal Government Operating Revenue" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void collectionExpendituresForEachLibraryDesc(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("library_name"),
                    String.valueOf(resultSet.getInt("print_collection_expenditures")),
                    String.valueOf(resultSet.getInt("digital_collection_expenditures")),
                    String.valueOf(resultSet.getInt("other_collection_expenditures")),
                    String.valueOf(resultSet.getInt("total_collection_expenditures")),
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "Library", "Print Collection Expenditures", "Digital Collection Expenditures", "Other Collection Expenditures", "Total Collection Expenditures" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void schoolsWithStateTotalPopulation(Query query) {

        if (applyCacheIfPresent(query.header)) return;

        try {
            PreparedStatement statement = connection.prepareStatement(query.body);
            ResultSet resultSet = statement.executeQuery();
            List<String[]> results = new ArrayList<>();
            while (resultSet.next()) {
                String[] result = {
                    resultSet.getString("school_name"),
                    resultSet.getString("state_alpha_code"),
                    String.valueOf(resultSet.getInt("state_population"))
                };
                results.add(result);
            }

            if (results.isEmpty()) displayNotFound();
            else {
                String[][] records = toTableRecords(results);
                String[] header = { "School", "State Alpha Code", "State Population" };
                UI.tabulate(records, header, query.header);
                cache.put(query.header, new CacheRecord(query.header, header, records));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

final class NumbersUtil {

    static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    static boolean isDouble(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

// Data
final class DatabaseLoader {

    private String connectionUrl;
    private TableSeedersExecutor tableSeedersExecutor;
    
    private DatabaseLoader(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        this.tableSeedersExecutor = new TableSeedersExecutor(connectionUrl);
    }

    public static DatabaseLoader up(String connectionUrl) {
        return new DatabaseLoader(connectionUrl);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.CAPITAL_REVENUES);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.COLLECTION_EXPENDITURES);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.COUNTIES);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.DATABASES_COUNTS);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.EMPLOYEE_EXPENDITURES);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.LIBRARIES);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.OPERATING_REVENUES);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.SCHOOLS);

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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.STAFF_MEMBERS_COUNTS);
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
        PreparedStatement insertion = connection.prepareStatement(SqlInsertion.STATES);
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

final class SqlInsertion {

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
