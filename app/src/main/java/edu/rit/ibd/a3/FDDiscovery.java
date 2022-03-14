package edu.rit.ibd.a3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

import com.google.common.collect.Sets;
import com.mysql.jdbc.JDBC4ResultSet;
import com.sun.jdi.request.StepRequest;
import jdk.jfr.consumer.RecordingStream;

public class FDDiscovery {
	public static void main(String[] args) throws SQLException, FileNotFoundException {
//		final String url = "jdbc:mysql://localhost:3306/Assingment3";
//		final String user = "root";
//		final String pwd = "";
//		final String relationName = "movie";
//		final String outputFile = "/Users/dibyanshuchatterjee/Downloads/BigData Assingments/Assingment 3/Output Doccument";

		final String url = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String relationName = args[3];
		final String outputFile = args[4];

		Connection con = DriverManager.getConnection(url,user,pwd);
		Set<String> attributes = new HashSet<>();
		Set<String> fds = new HashSet<>();
		PreparedStatement st = con.prepareStatement("SELECT * FROM " + relationName);
		ResultSet rs = st.executeQuery();
		attributes.addAll(readAttributes(st, rs)); //function to parse attributes
		st.close();
		rs.close();
		Map<String, List<Set<String>>> map = new HashMap<>();
		map.putAll(populateMapAndRemoveTrivial(attributes));

//		for (Map.Entry<String, List<Set<String>>> e: map.entrySet()){
//			System.out.println(e.getValue() + "--->" + e.getKey());
//		}
		Map<String, List<Set<String>>> minimal = new HashMap<>();
		minimal.putAll(getMinimal(map,relationName,con));
		System.out.println("=====");
		System.out.println(minimal);
		System.out.println("=====");

		//fds.addAll(createAndRunQueries(minimal, relationName,con));


		PrintWriter writer = new PrintWriter(new File(outputFile));
				System.out.println("printing fds = ");
				for (Map.Entry<String, List<Set<String>>> fd : minimal.entrySet()) {
					List<Set<String>> hhh = new ArrayList<>(fd.getValue());
					for (Set<String> kk : hhh) {
						ArrayList<String> ll = new ArrayList<>(kk);
						Collections.sort(ll);
						String l = String.join(", ", ll);
						String lk = l +" -> " + fd.getKey().toString();
						writer.println(lk);
					}
				}
				writer.close();


	}

	public static Map<String, List<Set<String>>> getMinimal(Map<String, List<Set<String>>> map, String relationName, Connection con) throws SQLException {

		Map<String, List<Set<String>>> minimal = new HashMap<>();
//		for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
//			minimal.put(entry.getKey(), new HashSet<>());
//			boolean flag = true;
//			for (String en : entry.getValue()) {
//				if (minimal.get(entry.getKey()).contains(en)) {
//					flag = false;
//					break;
//				}
//			}
//			if (flag) {
//				minimal.put(entry.getKey(), entry.getValue());
//			}
//		}
//		System.out.println(minimal);
//		return minimal;

		for (Map.Entry<String, List<Set<String>>> entry : map.entrySet()) {
			System.out.println("88 = " + entry);
//			if (!minimal.containsKey(entry.getKey())){// a,b -> totalVotes
//				minimal.put(entry.getKey(), new ArrayList<>());
//			}
			List <Set<String>>tempList = new ArrayList<>();

			for (Set<String> sets: entry.getValue()){
				System.out.println("79 = "+sets);
				System.out.println("89 = "+tempList);
				if(checkQuery(sets,entry.getKey(), relationName,con)) {
					if (!minimal.containsKey(entry.getKey())) {
						tempList.add(sets);
						minimal.put(entry.getKey(), tempList);
					} else {
						boolean gg = false;
						for (Set<String> set : tempList) {
							//System.out.println("94 = "+set);
							if (sets.containsAll(set)) {
								if (!gg) gg = true;
							}
						}


						if (!gg) {
							List<Set<String>> tt = new ArrayList<>(tempList);
							tt.add(sets);
							minimal.put(entry.getKey(), tt);
						}
					}


					tempList = new ArrayList<>(minimal.get(entry.getKey()));


				}


			//
			//}
			// for (Set<String> set : minimal.get(entry.getKey()){
			// if !sets.containsAll(set){
			// add it to the minimal
		//}
			//}







//			else { // totalVotes == true a,b,c -> totalVotes
				//				for (Set<String> sets: entry.getValue()){ //

				// if

//					for (Set<String> sets1: entry.getValue()){
//
//						if (!sets1.equals(sets)){
//
//							// if ( rhs is same && lhs contains all which were already present
//							if (sets1.containsAll(sets)){
//								entry.getValue().remove(sets1);
//							}
//							if (sets.containsAll(sets1)){
//								entry.getValue().remove(sets);
//							}
//						}
//					}
//				}
				// ab -> c
				// abf -> t
			}
//			minimal.put(entry.getKey(), entry.getValue());
//			System.out.println("102 = " + minimal);
		}

		return minimal;
	}

	private static boolean checkQuery(Set<String> sets, String key, String relationName, Connection con) throws SQLException {


		String stmt = "SELECT * FROM " + relationName + " AS t1" +
				" JOIN " + relationName + " AS t2 WHERE ";

				StringBuilder otherHalf = new StringBuilder();
				int ands = 0;
				for (String str : sets) {

					otherHalf.append("t1.").append(str).append("=").append("t2.").append(str);
					ands++;

					//if (ands < sets.size()) {
					otherHalf.append(" AND ");
					//}


				}
				//System.out.println(" ands = " + ands + " szie = " + entry.getValue().size());
				otherHalf.append(" t1.").append(key).append(" <> t2.").append(key).append(" LIMIT 1");
				//System.out.println(stmt + otherHalf);
				PreparedStatement query = con.prepareStatement(stmt + otherHalf);
				ResultSet resultSet = query.executeQuery();
				if (!resultSet.next()) { //resultSet.isBeforeFirst() && resultSet.getRow() == 0
					return true;
				}

		return false;
	}


	public static Map<String, List<Set<String>>> populateMapAndRemoveTrivial(Set<String> attributes){
		Map<String, List<Set<String>>> map = new HashMap<>();
		for (String str: attributes){
			map.put(str, new ArrayList<Set<String>>());
		}

		for (int size = 1; size < attributes.size(); size++){
			for (Set<String> leftHandSide : Sets.combinations(attributes, size)) {
				for (String str:attributes){
					if (!leftHandSide.contains(str)){
						map.get(str).add(leftHandSide);
						map.put(str,map.get(str)); //may not work
					}
				}
			}
		}


		return map;
	}


	public  static Set<String> readAttributes(PreparedStatement st, ResultSet rs) throws SQLException {
		Set<String> attributes = new HashSet<>();
		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
			attributes.add(rs.getMetaData().getColumnName(i));
		st.close();
		rs.close();
		return attributes;

	}

	public static Set<String> createAndRunQueries(Map<String, List<Set<String>>> minimal, String relationName, Connection con) throws SQLException {

				Set<String> result = new HashSet<>();
//		String stmt = "SELECT * FROM " + relationName + " AS t1" +
//				" JOIN " + relationName + " AS t2 WHERE ";
//		for (Map.Entry<String, Set<String>> entry : minimal.entrySet()) {
//			int ands = 0; //FIXME: going forward with non trivial and non minimal
//			StringBuilder otherHalf = new StringBuilder();
//			for (String atr : entry.getValue()) {
//				otherHalf.append("t1.").append(atr).append("=").append("t2.").append(atr);
//				ands++;
//				System.out.println(" ands = " + ands + " szie = " + entry.getValue().size());
//				if (ands < entry.getValue().size()) {
//					System.out.println("181 = " + entry.getValue().size() + "and = " + ands);
//					otherHalf.append(" AND ");
//				}
//
//			}
//			otherHalf.append(" AND t1.").append(entry.getKey()).append(" <> t2.").append(entry.getKey()).append(" LIMIT 1");
//
////			stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
////					ResultSet.CONCUR_READ_ONLY);
////			stmt.setFetchSize(Integer.MIN_VALUE);
//
//			PreparedStatement query = con.prepareStatement(stmt + otherHalf);
//			System.out.println(stmt + otherHalf);
////			query.setFetchSize(1);
//			ResultSet resultSet = query.executeQuery();//FIXME: ResultSet having issues
//			System.out.println("size  ==" + resultSet.getFetchSize());
//			if (resultSet.getFetchSize() == 0) { //resultSet.isBeforeFirst() && resultSet.getRow() == 0
//				result.put(entry.getKey(), entry.getValue());
//			}
//			resultSet.close();
//			query.close();
////				System.out.println(stmt + otherHalf); //TODO: checks the queries
////					System.out.println();
////					System.out.println();

		String stmt = "SELECT * FROM " + relationName + " AS t1" +
				" JOIN " + relationName + " AS t2 WHERE ";
		for (Map.Entry<String, List<Set<String>>> entry : minimal.entrySet()) {
			for (Set<String> sets : entry.getValue()) {

				StringBuilder otherHalf = new StringBuilder();
				int ands = 0;
				for (String str : sets) {

					otherHalf.append("t1.").append(str).append("=").append("t2.").append(str);
					ands++;

					//if (ands < sets.size()) {
						otherHalf.append(" AND ");
					//}


				}
				//System.out.println(" ands = " + ands + " szie = " + entry.getValue().size());
				otherHalf.append(" t1.").append(entry.getKey()).append(" <> t2.").append(entry.getKey()).append(" LIMIT 1");
				//System.out.println(stmt + otherHalf);
				PreparedStatement query = con.prepareStatement(stmt + otherHalf);
				ResultSet resultSet = query.executeQuery();
							if (!resultSet.next()) { //resultSet.isBeforeFirst() && resultSet.getRow() == 0
								String LHS = "";
								int comma = 0;
								for (String str:sets){
									comma++;
									LHS += str;
									if (comma < sets.size()) {
										LHS += ", ";
									}
								}

								String RHS = " -> " + entry.getKey();
								result.add(LHS + RHS);
			}
			}

		}

		return result;
	}
	}

