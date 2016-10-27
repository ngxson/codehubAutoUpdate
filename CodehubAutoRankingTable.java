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
	static final String USER = "homestead";
	static final String PASS = "secret";

	// delay time between updates
	static int DELAY = 15; // default delay in minutes
	static final boolean DEBUG = false;

	static int RunTimes = 0;
	static Connection conn = null;
	static Statement stmt = null;

	static String sql = "select rankingTable.userId, rankingTable.username, CONCAT(rankingTable.firstname, \" \", rankingTable.lastname) AS fullname, rankingTable.totalScore AS score, rankingTable.rank from " +
			"(select userId, username, totalScore, firstname, lastname, " +
			"  @curRank := IF(@prevRank = totalScore, @curRank, @incRank) AS rank, " +
			"  @incRank := @incRank + 1, " +
			"  @prevRank := totalScore " +
			"from " +
			"  (select b.userId as userId, b.username as username, sum(b.maxScore) as totalScore, b.firstname, b.lastname " +
			"  from " +
			"    (select users.userId, users.username, submissions.problemId, submissions.courseId, users.firstname, users.lastname,  COALESCE(max(resultScore),-1) as maxScore " +
			"    from submissions right join users on submissions.userId = users.userId " +
			"    group by users.userId, problemId, courseId, users.firstname, users.lastname) as b " +
			"  group by b.userId order by totalScore desc) as c, " +
			"  (SELECT @curRank :=0, @prevRank := NULL, @incRank := 1) as r " +
			") as rankingTable ";

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
			sql_create = "create table if not exists rankingtable(" +
					"userId int PRIMARY KEY, " +
					"username char(200), " +
					"fullname char(200), " +
					"score int, " +
					"rank int);";
			PreparedStatement pst = (PreparedStatement) conn.prepareStatement(sql_create);
			pst.executeUpdate();
			pst = (PreparedStatement) conn.prepareStatement(
				"ALTER TABLE rankingtable CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;");
			pst.executeUpdate();

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
			String uname, fullname;

			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				uid = rs.getInt("userId");
				uname = rs.getString("username");
				fullname = rs.getString("fullname");
				score = rs.getInt("score");
				rank = rs.getInt("rank");

				// Display values
				if(DEBUG) System.out.println(
					"uid: " + uid +
					", fullname: " + fullname +
					", rank: " + rank);

				// insert
				PreparedStatement pst_del = (PreparedStatement) conn.prepareStatement(
						"DELETE FROM rankingtable WHERE userId=" + uid);
				pst_del.executeUpdate();
				PreparedStatement pstt = (PreparedStatement) conn.prepareStatement(
						"INSERT INTO rankingtable (userId, username, fullname, score, rank) VALUES("
								+ uid + ",\"" + uname + "\",\"" + fullname + "\"," + score + "," + rank + "); ");
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