import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

public class CodehubSeederScoreTable {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/uetcodehub";

	// Database credentials
	static String USER = "homestead";
	static String PASS = "secret";

	// delay time between updates
	static int DELAY = 10; // default delay in minutes
	static final boolean DEBUG = false;

	static int RunTimes = 0;
	static Connection conn = null;
	static Statement stmt = null;

	static String sql = "SELECT userId, problemId, COALESCE(examId, -1) AS examId, courseId, COALESCE(MAX(resultScore), -1) AS maxScore, submitId "+
			"FROM submissions " +
			"GROUP BY userId, problemId, examId, courseId";

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		USER = args[0];
		PASS = args[1];

		// STEP 2: Register JDBC driver
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// STEP 3: Open a connection
			System.out.println("Connecting to database...");
			conn = (Connection) DriverManager.getConnection(DB_URL, USER, PASS);

			// STEP 4: Execute a query
			System.out.println("OK");
			stmt = (Statement) conn.createStatement();

			String sql_create;
			String sql;
			// Create table if not existed
			sql_create = "create table if not exists userproblemscore(Id int AUTO_INCREMENT PRIMARY KEY,"
				+" userId int, problemId int, examId int, courseId int, maxScore int, submitId int);";
			PreparedStatement pst = (PreparedStatement) conn.prepareStatement(sql_create);
			int numRowsChanged = pst.executeUpdate();
			
			//delete old data
			PreparedStatement pst_del = (PreparedStatement) conn.prepareStatement(
						"DELETE FROM userproblemscore");
			pst_del.executeUpdate();

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
			int uid, pid, eid, cid, score, sid;
			String sql_update;

			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				uid = rs.getInt("userId");
				pid = rs.getInt("problemId");
				eid = rs.getInt("examId");
				cid = rs.getInt("courseId");
				score = rs.getInt("maxScore");
				sid = rs.getInt("submitId");
				sql_update = "INSERT INTO userproblemscore (" + "userId, problemId, examId, courseId, maxScore, submitId) VALUES" + "("
						+ uid + "," + pid + "," + eid + "," + cid + "," + score + "," + sid + "); ";

				// Display values
				if (DEBUG) {
					System.out.println(sql_update);
				}

				// insert
				/*PreparedStatement pst_del = (PreparedStatement) conn.prepareStatement(
						"DELETE FROM userproblemscore WHERE courseProblemId=" + cpid);
				pst_del.executeUpdate();*/
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
	}
}