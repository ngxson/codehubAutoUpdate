import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

public class CodehubAutoSolveTable {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/uetcodehub";

	// Database credentials
	static final String USER = "homestead";
	static final String PASS = "secret";

	// delay time between updates
	static int DELAY = 10; // default delay in minutes
	static final boolean DEBUG = false;

	static int RunTimes = 0;
	static Connection conn = null;
	static Statement stmt = null;

	static String sql = "SELECT tab1.problemId, tab1.numOfUser AS submittedUser, tab2.numOfUser AS finishedUser "
			+ "FROM (SELECT problemId, count(userId) as numOfUser from( "
			+ "	select problems.problemId, submissions.userId, max(submissions.resultScore) as userScore, submissions.courseId, problems.defaultScore "
			+ "	from problems left join submissions on problems.problemId = submissions.problemId "
			+ "	group by problems.problemId, submissions.userId) as s " + "group by problemId) as tab1 " + "LEFT JOIN "
			+ "(select problems.problemId, count(s.userId) as numOfUser from( "
			+ "	select problems.problemId, submissions.userId, max(submissions.resultScore) as userScore, submissions.courseId, problems.defaultScore "
			+ "	from problems left join submissions on problems.problemId = submissions.problemId "
			+ "	group by problems.problemId, submissions.userId having userScore = defaultScore) as s  "
			+ "	right join problems on s.problemId = problems.problemId " + "group by problems.problemId) as tab2 "
			+ "ON tab1.problemId=tab2.problemId";

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		if (args.length != 0) {
			System.out.println("timed=" + args[0]);
			DELAY = Integer.valueOf(args[0]);
		}

		// STEP 2: Register JDBC driver
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// STEP 3: Open a connection
			System.out.println("Connecting to database...");
			conn = (Connection) DriverManager.getConnection(DB_URL, USER, PASS);

			// STEP 4: Execute a query
			System.out.println("Creating statement...");
			stmt = (Statement) conn.createStatement();

			String sql_create;
			String sql;
			// Create table if not existed
			sql_create = "create table if not exists problemsolvingresult(" + "problemId int PRIMARY KEY,"
					+ " submittedUser int," + " finishedUser int);";
			PreparedStatement pst = (PreparedStatement) conn.prepareStatement(sql_create);
			int numRowsChanged = pst.executeUpdate();

			updateTable();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
	}

	public static void updateTable() {
		if (DEBUG) RunTimes++;
		try {
			ResultSet rs = stmt.executeQuery(sql);
			//PreparedStatement pst_del = (PreparedStatement) conn.prepareStatement("DELETE FROM problemsolvingresult");
			//pst_del.executeUpdate();
			int pid, submit, done;

			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				pid = rs.getInt("problemId");
				submit = rs.getInt("submittedUser");
				done = rs.getInt("finishedUser");

				// Display values
				if (DEBUG) System.out.println("pid: " + pid +
					", submit: " + submit +
					", done: " + done);

				// insert
				PreparedStatement pst_del = (PreparedStatement) conn.prepareStatement(
						"DELETE FROM problemsolvingresult WHERE problemId=" + pid);
				pst_del.executeUpdate();
				PreparedStatement pstt = (PreparedStatement) conn.prepareStatement(
						"INSERT INTO problemsolvingresult (" + "problemId, submittedUser, finishedUser) VALUES" + "("
								+ pid + "," + submit + "," + done + "); ");
				pstt.executeUpdate();
			}
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		}
		if (DEBUG) System.out.println("Runned: " + RunTimes);
		try {
			Thread.sleep(DELAY * 60000);
			updateTable();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}