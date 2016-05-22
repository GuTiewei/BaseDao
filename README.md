//BaseDao
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.just.entity.InsertKeyValues;
import com.just.entity.SetKeyValues;
import com.just.entity.WhereKeyValues;
import com.just.util.DButil;

public class BaseDao {
	private Connection con;

	/**
	 * 
	 * @param table
	 * @param selections
	 * @param whereKeyValues
	 * @return
	 */
	protected ResultSet select(String table, List<String> selections,
			WhereKeyValues whereKeyValues) {
		return this.select(table, selections, whereKeyValues.getWhereKeys(),
				whereKeyValues.getWhereValues());
	}

	/**
	 * 
	 * @param table
	 * @param whereKeyValues
	 * @return
	 */
	protected int delete(String table, WhereKeyValues whereKeyValues) {
		return this.delete(table, whereKeyValues.getWhereKeys(),
				whereKeyValues.getWhereValues());
	}

	/**
	 * 
	 * @param table
	 * @param setKeyValues
	 * @param whereKeyValues
	 * @return
	 */
	protected int update(String table, SetKeyValues setKeyValues,
			WhereKeyValues whereKeyValues) {
		return this.update(table, setKeyValues.getSetKeys(),
				setKeyValues.getSetValues(), whereKeyValues.getWhereKeys(),
				whereKeyValues.getWhereValues());
	}

	/**
	 * 
	 * @param table
	 * @param whereKeysValues
	 * @return
	 */
	protected int insert(String table, InsertKeyValues insertKeysValues) {
		return this.insert(table, insertKeysValues.getKeys(),
				insertKeysValues.getValues());
	}
	
	/**
	 * 
	 * @param table
	 * @return
	 * @throws SQLException
	 */
	protected int count(String table) throws SQLException {
		int n = 0;
		PreparedStatement pstmt = bulidSimpleCountStatement(table);
		if (null != pstmt) {
			ResultSet rs = pstmt.executeQuery();
			if (rs.first()) {
				n = rs.getInt(1);
			}
		}
		return n;
	}

	/**
	 * 
	 * @param table
	 * @param selections
	 * @param whereKeys
	 * @param whereValues
	 * @return
	 */
	private ResultSet select(String table, List<String> selections,
			List<String> whereKeys, List<Object> whereValues) {

		try {
			PreparedStatement pstmt = buildSimpleQueryStatement(table,
					selections, whereKeys, whereValues);
			if (null != pstmt)
				return pstmt.executeQuery();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param table
	 * @param whereKeys
	 * @param whereValues
	 * @return
	 */

	private int delete(String table, List<String> whereKeys,
			List<Object> whereValues) {

		try {
			PreparedStatement pstmt = bulidSimpleDeleteStatement(table,
					whereKeys, whereValues);
			if (null != pstmt)
				return pstmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 
	 * @param table
	 * @param setKeys
	 * @param setValues
	 * @param whereKeys
	 * @param whereValues
	 * @return
	 */
	private int update(String table, List<String> setKeys,
			List<Object> setValues, List<String> whereKeys,
			List<Object> whereValues) {

		try {
			PreparedStatement pstmt = bulidSimpleUpdateStatement(table,
					setKeys, setValues, whereKeys, whereValues);
			if (null != pstmt)
				return pstmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 
	 * @param table
	 * @param keys
	 * @param values
	 * @return
	 */
	private int insert(String table, List<String> keys, List<Object> values) {

		try {
			PreparedStatement pstmt = bulidSimpleInsertStatement(table, keys,
					values);
			if (null != pstmt)
				return pstmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}


	/**
	 * 拼接查询SQL语句
	 * 
	 * @param table
	 * @param selections
	 * @param keys
	 * @param values
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement buildSimpleQueryStatement(String table,
			List<String> selections, List<String> whereKeys,
			List<Object> whereValues) throws SQLException {
		con = DButil.getCon();
		String sql = "SELECT ";
		if (selections == null || selections.size() == 0)
			sql += "* ";
		else {
			for (String sel : selections) {
				sql += sel + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
		}
		sql += " FROM " + table;

		if (whereKeys != null && whereKeys.size() > 0) {
			sql += " WHERE ";
			for (int i = 0; i < whereKeys.size(); i++) {
				sql += whereKeys.get(i) + "=" + "? AND ";
			}
			sql = sql.substring(0, sql.length() - 4);
			PreparedStatement pstmt = con.prepareStatement(sql);
			for (int i = 0; i < whereKeys.size(); i++) {
				prepareValue(pstmt, i + 1, whereValues.get(i));
			}
			return pstmt;
		} else {
			return con.prepareStatement(sql);
		}
	}

	/**
	 * 
	 * @param table
	 * @param whereKeys
	 * @param whereValues
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement bulidSimpleDeleteStatement(String table,
			List<String> whereKeys, List<Object> whereValues)
			throws SQLException {
		con = DButil.getCon();
		String sql = "DELETE FROM " + table;
		if (whereKeys != null && whereKeys.size() > 0) {
			sql += " WHERE ";
			for (int i = 0; i < whereKeys.size(); i++) {
				sql += whereKeys.get(i) + "=" + "? AND ";
			}
			sql = sql.substring(0, sql.length() - 4);
			PreparedStatement pstmt = con.prepareStatement(sql);
			for (int i = 0; i < whereKeys.size(); i++) {
				prepareValue(pstmt, i + 1, whereValues.get(i));
			}
			return pstmt;
		}
		return null;
	}

	/**
	 * 
	 * @param table
	 * @param setKeys
	 * @param setValues
	 * @param whereKeys
	 * @param whereValues
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement bulidSimpleUpdateStatement(String table,
			List<String> setKeys, List<Object> setValues,
			List<String> whereKeys, List<Object> whereValues)
			throws SQLException {
		con = DButil.getCon();
		String sql = "UPDATE " + table;
		if (setKeys != null && setKeys.size() > 0) {
			sql += " SET ";
			for (int i = 0; i < setKeys.size(); i++) {
				sql += setKeys.get(i) + "=?" + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
		}

		if (whereKeys != null && whereKeys.size() > 0) {
			sql += " WHERE ";
			for (int i = 0; i < whereKeys.size(); i++) {
				sql += whereKeys.get(i) + "=" + "? AND ";
			}
			sql = sql.substring(0, sql.length() - 4);
			PreparedStatement pstmt = con.prepareStatement(sql);
			List<Object> values = new ArrayList<Object>();
			values.addAll(setValues);
			values.addAll(whereValues);
			for (int i = 0; i < values.size(); i++) {
				prepareValue(pstmt, i + 1, values.get(i));
			}
			return pstmt;
		}
		return null;
	}

	/**
	 * 
	 * @param table
	 * @param keys
	 * @param values
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement bulidSimpleInsertStatement(String table,
			List<String> keys, List<Object> values) throws SQLException {
		// TODO Auto-generated method stub
		con = DButil.getCon();
		String sql = "INSERT INTO " + table + "(";
		String str1 = "";
		String str2 = "";
		if (keys != null && keys.size() > 0) {
			for (int i = 0; i < keys.size(); i++) {
				str1 += keys.get(i) + ",";
				str2 += "?,";
			}
			sql = sql + str1.substring(0, str1.length() - 1) + ") VALUES("
					+ str2.substring(0, str2.length() - 1) + ")";

			PreparedStatement pstmt = con.prepareStatement(sql);
			for (int i = 0; i < keys.size(); i++) {
				prepareValue(pstmt, i + 1, values.get(i));
			}
			return pstmt;
		}

		return null;
	}

	private PreparedStatement bulidSimpleCountStatement(String table)
			throws SQLException {
		// TODO Auto-generated method stub
		con = DButil.getCon();
		String sql = "SELECT COUNT(*) FROM " + table;
		return con.prepareStatement(sql);
	}

	/**
	 * 设置值
	 * 
	 * @param pstmt
	 * @param index
	 * @param value
	 * @throws SQLException
	 */
	private void prepareValue(PreparedStatement pstmt, int index, Object value)
			throws SQLException {
		if (value instanceof Integer) {
			pstmt.setInt(index, ((Integer) value).intValue());
		} else if (value instanceof String) {
			pstmt.setString(index, value.toString());
		} else if (value instanceof Date) {
			pstmt.setDate(index, new java.sql.Date(((Date) value).getTime()));
		} else pstmt.setObject(index, value);
	}
}

