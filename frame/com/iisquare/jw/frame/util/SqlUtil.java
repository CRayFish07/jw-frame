package com.iisquare.jw.frame.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL语句处理类
 * @author Ouyang <iisquare@163.com>
 *
 */
public class SqlUtil {
	
	public static final String sqlCountName = "COUNT(*)";
	public static final String regexSelectFrom = "^((?i)select\\b)(.+?)(\\b(?i)from\\b)";

	public static String convertForCount(String sql) {
		return convertForCount(sql, null);
	}
	
	/**
	 * 将select语句转换为select COUNT(*)语句
	 * 如需要去重等操作，建议在where条件中处理
	 */
	public static String convertForCount(String sql, String countName) {
		StringBuilder sb = new StringBuilder("$1 ").append(sqlCountName);
		if(!DPUtil.empty(countName)) {
			sb.append(" as ").append(countName);
		}
		sb.append(" $3");
		sql = sql.replaceFirst(regexSelectFrom, sb.toString());
		return sql;
	}

	/**
	 * 创建安全where查询语句
	 */
	public static String buildSafeWhere(String split, String field, Object[] values, String operator) {
		String[] strs = DPUtil.filterArray(values, "'", true, true, true, true);
		if(DPUtil.empty(strs)) return "";
		if(DPUtil.empty(operator)) operator = "=";
		split = DPUtil.stringConcat(" ", split, " ");
		operator = DPUtil.stringConcat(" ", operator, " ");
		List<String> list = new ArrayList<String>(strs.length);
		for (Object str : strs) {
			list.add(DPUtil.stringConcat(field, operator, str));
		}
		return DPUtil.implode(split, DPUtil.collectionToArray(list));
	}
	
	/**
	 * 创建安全where查询语句
	 */
	public static String buildSafeWhere(String split, Object[] values) {
		String[] strs = DPUtil.filterArray(values, "'", true, true, true, true);
		return DPUtil.implode(split, strs);
	}
	
	public static String buildWhere(Object[] keys, Object[] operators, boolean bPlaceholder) {
		if(DPUtil.empty(keys)) return "";
		int length = keys.length;
		if(DPUtil.empty(operators)) {
			operators = DPUtil.getFillArray(length, "=");
		}
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < length; i++) {
			if(i > 0) {
				sb.append(" and ");
			}
			sb.append(keys[i]);
			String operator = DPUtil.trim(DPUtil.parseString(operators[i]).toLowerCase());
			sb.append(" ").append(operator).append(" ");
			if(bPlaceholder) {
				sb.append("?");
			} else {
				sb.append(":").append(keys[i]);
			}
		}
		return sb.toString();
	}
	
	public static String buildWhere(Map<String, Object> where, Map<String, String> operators) {
		if(DPUtil.empty(where)) return "";
		if(DPUtil.empty(operators)) {
			return buildWhere(where.keySet().toArray(), null, false);
		} else {
			/* Map内部元素顺序随机 */
			String[] keys = DPUtil.collectionToStringArray(where.keySet());
			int length = keys.length;
			String[] values = new String[length];
			for(int i = 0; i < length; i++) {
				values[i] = operators.get(keys[i]);
			}
			return buildWhere(keys, values, false);
		}
	}
	
	public static Map<String, Object> buidNamedMap(String[] keys, Object[] values) {
		Map<String, Object> map = new HashMap<String, Object>(DPUtil.parseInt(keys.length / 0.75f));
		for(int i = 0; i < keys.length; i++) {
			map.put(keys[i], values[i]);
		}
		return map;
	}
	
	public static String buildLimit(int page, int pageSize) {
		if(pageSize < 1) return "";
		StringBuilder sb = new StringBuilder();
		if(page < 1) page = 1;
		page = (page - 1) * pageSize;
		sb.append(" limit ").append(page).append(", ").append(pageSize);
		return sb.toString();
	}
	
	public static String buildSelect(String tableName,
			String fields, String where, String append, int page, int pageSize) {
		StringBuilder sb = new StringBuilder("select ").append(fields).append(" from ").append(tableName);
		if(!DPUtil.empty(where)) {
			sb.append(" where ").append(where);
		}
		if(null != append) sb.append(" ").append(append).append(buildLimit(page, pageSize));
		return sb.toString();
	}
	
	public static String buildInsert(String tableName, Object[] keys, boolean bPlaceholder) {
		StringBuilder sbFileds = new StringBuilder();
		StringBuilder sbValues = new StringBuilder();
		for(int i = 0; i < keys.length; i++) {
			if(i > 0) {
				sbFileds.append(", ");
				sbValues.append(", ");
			}
			sbFileds.append(keys[i]);
			if(bPlaceholder) {
				sbValues.append("?");
			} else {
				sbValues.append(":").append(keys[i]);
			}
		}
		StringBuilder sb = new StringBuilder("insert into ").append(tableName).append(" (")
				.append(sbFileds.toString()).append(") values (").append(sbValues.toString()).append(")");
		return sb.toString();
	}
	
	public static String buildInsert(String tableName, Map<String, Object> values) {
		return buildInsert(tableName, values.keySet().toArray(), false);
	}
	
	public static String buildUpdate(String tableName, Object[] keys, String where, boolean bPlaceholder) {
		StringBuilder sb = new StringBuilder("update ").append(tableName).append(" set ");
		for(int i = 0; i < keys.length; i++) {
			if(i > 0) {
				sb.append(", ");
			}
			sb.append(keys[i]);
			if(bPlaceholder) {
				sb.append("=?");
			} else {
				sb.append("=:").append(keys[i]);
			}
		}
		if(!DPUtil.empty(where)) {
			sb.append(" where ").append(where);
		}
		return sb.toString();
	}
	
	public static String buildUpdate(String tableName, Map<String, Object> values, String where) {
		return buildUpdate(tableName, values.keySet().toArray(), where, false);
	}
	
	public static String buildDelete(String tableName, String where) {
		StringBuilder sb = new StringBuilder("delete from ").append(tableName);
		if(!DPUtil.empty(where)) {
			sb.append(" where ").append(where);
		}
		return sb.toString();
	}
	
	public static String buildTruncate(String tableName) {
		StringBuilder sb = new StringBuilder("truncate tableName ").append(tableName);
		return sb.toString();
	}
}
