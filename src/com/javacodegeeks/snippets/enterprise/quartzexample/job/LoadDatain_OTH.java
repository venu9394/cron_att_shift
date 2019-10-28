package com.javacodegeeks.snippets.enterprise.quartzexample.job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadDatain_OTH {

	PreparedStatement pstmt=null;
	ResultSet rs=null;
	Connection con=Util.getConnection();
	
	
	public void Load() {
		
		String Query="SELECT * FROM `procedure`.tbl_employee_logs where employeeid=11778 and transactiondate in('2019-05-18','2019-05-19')\r\n" + 
				"order by transactiontime;";
		
		
		  try {
			pstmt=con.prepareStatement(Query);
			 rs = pstmt.executeQuery();
			 
			 rs.last();
			 int numberOfRows = rs.getRow();
			 
			 System.out.println(numberOfRows +"-*********numberOfRows");
			 rs.beforeFirst();
			 int count=0;
			 while(rs.next()) {
				 
				 System.out.println("ROW1"+rs.getString(3));
				 System.out.println("Count Table :::" +count);
				 if(rs.next()) {
					 System.out.println("ROW2"+rs.getString(3));
					 
				 }else {
					 System.out.println("ROW3- last row faild..!");
				 }
				 
				 count ++;
			 }
			 
			 
			 
			 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		
	}
	
	
	
	public static void main(String [] args) {
		
		LoadDatain_OTH obj=new LoadDatain_OTH();
		obj.Load();
		
		
	}
}
