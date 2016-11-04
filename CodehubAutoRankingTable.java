import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

public class CodehubAutoRankingTable {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/uetcodehub?useUnicode=yes&characterEncoding=UTF-8";

	// Database credentials
	static String USER = "homestead";
	static String PASS = "secret";

	// delay time between updates
	static int DELAY = 15; // default delay in minutes
	static final boolean DEBUG = false;

	static int RunTimes = 0;
	static Connection conn = null;
	static Statement stmt = null;

	// rankingTable.username, CONCAT(rankingTable.firstname, \" \", rankingTable.lastname) AS fullname,
	static String sql = "SELECT rankingTable.userId, rankingTable.totalScore AS score, rankingTable.rank FROM "+
				"	(SELECT userId, totalScore, "+
				"		@curRank := IF(@prevRank = totalScore, @curRank, @incRank) AS rank,  "+
				"		@incRank := @incRank + 1,  "+
				"		@prevRank := totalScore "+
				"		FROM "+
				"		(SELECT userId, SUM(maxScore) AS totalScore "+
				"		FROM userproblemscore "+
				"		WHERE examId=-1 "+
				"		GROUP BY userId ORDER BY totalScore DESC) AS c, "+
				"	(SELECT @curRank :=0, @prevRank := NULL, @incRank := 1) AS r  "+
				") AS rankingTable";

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		int first_delay = 1;
		
		/*
			args[0]: Delay to start program (in second)
			args[1]: Delay between 2 queries (in minute)
			args[2]: User name
			args[3]: Password
		*/
		
		if (args.length != 0) {
			System.out.println("delay=" + args[0]);
			first_delay = Integer.valueOf(args[0]);
			System.out.println("timed=" + args[1]);
			DELAY = Integer.valueOf(args[1]);
			USER = args[2];
			PASS = args[3];
		}
		
		try {
			Thread.sleep(first_delay * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
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
			sql_create = "create table if not exists rankingtable(" +
					"userId int PRIMARY KEY, " +
					"score int, " +
					"rank int);";
			PreparedStatement pst = (PreparedStatement) conn.prepareStatement(sql_create);
			pst.executeUpdate();
			//pst = (PreparedStatement) conn.prepareStatement(
			//	"ALTER TABLE rankingtable CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;");
			//pst.executeUpdate();

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
			int uid, score, rank;

			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				uid = rs.getInt("userId");
				score = rs.getInt("score");
				rank = rs.getInt("rank");

				// Display values
				if(DEBUG) System.out.println(
					"uid: " + uid +
					", rank: " + rank);

				// insert
				PreparedStatement pst_del = (PreparedStatement) conn.prepareStatement(
						"DELETE FROM rankingtable WHERE userId=" + uid);
				pst_del.executeUpdate();
				PreparedStatement pstt = (PreparedStatement) conn.prepareStatement(
						"INSERT INTO rankingtable (userId, score, rank) VALUES("
								+ uid + "," + score + "," + rank + "); ");
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