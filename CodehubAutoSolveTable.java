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

	static String sql = "SELECT tab1.courseProblemId, tab1.problemId, tab1.courseId," + 
			"COALESCE(tab1.numOfUser, 0) AS submittedUser," + 
			"COALESCE(tab2.numOfUser, 0) AS finishedUser FROM " + 
			"	(SELECT courseProblemId, problemId, courseId, count(userId) as numOfUser from( " + 
			"		SELECT tab2.courseProblemId, tab2.problemId, submissions.userId, max(submissions.resultScore) as userScore, submissions.courseId " + 
			"		FROM courseproblems AS tab2 " + 
			"		left join submissions " + 
			"		on tab2.problemId = submissions.problemId AND tab2.courseId = submissions.courseId " + 
			"		WHERE tab2.isActive=1 " + 
			"		GROUP BY tab2.courseProblemId, submissions.userId) " + 
			"	as s " + 
			"	GROUP BY courseProblemId " + 
			"	) as tab1 " + 
			"LEFT JOIN " + 
			"	(SELECT courseProblemId, problemId, courseId, count(userId) as numOfUser from( " + 
			"		SELECT tab2.courseProblemId, tab2.problemId, submissions.userId, max(submissions.resultScore) as userScore, submissions.courseId, tab2.defaultScore " + 
			"		FROM  " + 
			"			(SELECT cp.courseProblemId, cp.problemId, p.defaultScore, cp.isActive, cp.courseId  " + 
			"			FROM courseproblems AS cp " + 
			"			LEFT JOIN problems AS p " + 
			"			ON cp.problemId = p.problemId " + 
			"			WHERE cp.isActive=1	 " + 
			"			) AS tab2 " + 
			"		left join submissions " + 
			"		on tab2.problemId = submissions.problemId AND tab2.courseId = submissions.courseId " + 
			"		GROUP BY tab2.courseProblemId, submissions.userId " + 
			"		HAVING userScore = defaultScore) " + 
			"	as s " + 
			"	GROUP BY courseProblemId " + 
			"	) as tab2 " + 
			"ON tab1.courseProblemId=tab2.courseProblemId " + 
			"WHERE tab1.courseId IS NOT NULL";

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
			sql_create = "create table if not exists problemsolvingresult(" + "courseProblemId int PRIMARY KEY," + 
					" problemId int," + " courseId int," +
					" submittedUser int," + " finishedUser int);";
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
			int cpid, pid, cid, submit, done, stt=0;
			String sql_update;

			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				cpid = rs.getInt("courseProblemId");
				pid = rs.getInt("problemId");
				cid = rs.getInt("courseId");
				submit = rs.getInt("submittedUser");
				done = rs.getInt("finishedUser");
				sql_update = "INSERT INTO problemsolvingresult (" + "courseProblemId, problemId, courseId, submittedUser, finishedUser) VALUES" + "("
						+ cpid + "," + pid + "," + cid + "," + submit + "," + done + "); ";

				// Display values
				if (DEBUG) {
					stt++;
					System.out.println(stt+sql_update);
				}

				// insert
				PreparedStatement pst_del = (PreparedStatement) conn.prepareStatement(
						"DELETE FROM problemsolvingresult WHERE courseProblemId=" + cpid);
				pst_del.executeUpdate();
				PreparedStatement pstt = (PreparedStatement) conn.prepareStatement(sql_update);
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