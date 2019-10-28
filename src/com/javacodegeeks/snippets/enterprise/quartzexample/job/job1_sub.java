package com.javacodegeeks.snippets.enterprise.quartzexample.job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class job1_sub {
	  
public synchronized Map calljob (int minLimit, int MaxLimit, Connection con,String FETCHQUERY,String INSERTQURY,int CallNum,Connection conSql,String Masql_FETCHQUERY){
	
	 //System.out.println("15");
		
		Map  map=new HashMap()  ;
		int[] excutBatch;
		ArrayList empiid=new ArrayList();
		Map parrams=new HashMap();
		try{
			
			//System.out.println("12");
			boolean flag=false;
			int MiniLimit_m=MaxLimit+1;
			int MaxLimit_m=MaxLimit+100;
			String DATEON=null;
			String DAYTYPE=null;
			
			map.put("minLimit", ""+MiniLimit_m+"");
			map.put("MaxLimit", ""+MaxLimit_m+"");
			
			//String Query=Job1.fetchQuery;
			
			/*PreparedStatement pstmt = con.prepareStatement( Query.toString());*/
			
			
			
			/*select IFNULL(A.HOLIDAYDATE,'0000-00-00') HOLIDAYDATE
			from HCLHRM_PROD.TBL_HOLIDAYS A left join
			HCLHRM_PROD.TBL_EMPLOYEE_PRIMARY B ON A.BUSINESSUNITID=B.companyid
			where A.employeeid in(20314)*/
			
			//System.out.println("2");
			PreparedStatement pstmt=null;
		     pstmt = con.prepareStatement(FETCHQUERY);
		   
		//   Masql_FETCHQUERY= "select  A.AttendanceDate , B.EmployeeCode, B.EmployeeId , B.EmployeeName ,convert(varchar,CAST(A.InTime as datetime),8) as intime,convert(varchar,CAST(A.OutTime as datetime),8) as outtime , A.LateBy , A.EarlyBy , A.ShiftId,A.Duration ,convert(varchar(5),DateDiff(s, A.InTime, A.outTime)/3600)+':'+convert(varchar(5),DateDiff(s, A.InTime, A.outTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, A.InTime, A.outTime)%60)) workinghours from dbo.AttendanceLogs  A left join dbo.Employees  B ON B.employeeid=A.employeeid where A.AttendanceDate in(?) and B.EmployeeCode in(?)";
		  
		   StringBuffer Buffatt=new StringBuffer();
		   StringBuffer Holidays=new StringBuffer();
	      
	       
	         
	        /* Buffatt.append(" select  A.AttendanceDate ,B.EmployeeCode, B.EmployeeId , B.EmployeeName ,A.InTime as intime,A.OutTime as outtime , A.LateBy , A.EarlyBy , A.ShiftId,A.Duration As workinghours from dbo.AttendanceLogs  A ");
	         Buffatt.append(" left join dbo.Employees  B ON B.employeeid=A.employeeid where A.AttendanceDate in(?) and B.EmployeeCode in(?) ");
	        
	         */
		  // PreparedStatement MsSqlpstmt = null;
		   Statement MsSqlpstmt = conSql.createStatement();
		   Statement MsSqlpstmt1 = conSql.createStatement();
		 
		   ResultSet rs=null;
		   
		   pstmt.setInt(1, minLimit);   
		   System.out.println(pstmt);
		    
		    String Month=null;
		    String  Year=null;
	    	 
		    
		     PreparedStatement stmt=con.prepareStatement(INSERTQURY);
		     
		     StringBuffer INSERTQURY_2=new  StringBuffer();
		     
		  /*  VENU CHANGED-->20-05-2019 
		     INSERTQURY_2.append(" insert into procedure.tbl_employee_logs_allbu(EMPLOYEEID, TRANSACTIONDATE, TRANSACTIONTIME_IN,INOUT_STATUS,LASTUPDATE,TRANSACTIONTIME_OUT,NETTIME) values (?,?,?,?,now(),?,'00:00:00') ");
		     INSERTQURY_2.append(" on duplicate key update EMPLOYEEID=?,TRANSACTIONDATE=?, TRANSACTIONTIME_IN=? ,INOUT_STATUS=concat(INOUT_STATUS,?),LASTUPDATE=now(),TRANSACTIONTIME_OUT=?,NETTIME=timediff(TRANSACTIONTIME_OUT,TRANSACTIONTIME_IN) ");
		   */ 
		     INSERTQURY_2.append(" insert into procedure.tbl_employee_logs_allbu(EMPLOYEEID, TRANSACTIONDATE, TRANSACTIONTIME_IN,INOUT_STATUS,LASTUPDATE,TRANSACTIONTIME_OUT,NETTIME) values (?,?,?,?,now(),?, timediff(?,?)) ");
		     INSERTQURY_2.append(" on duplicate key update EMPLOYEEID=?,TRANSACTIONDATE=?, TRANSACTIONTIME_IN=? ,INOUT_STATUS=?,LASTUPDATE=now(),TRANSACTIONTIME_OUT=?,NETTIME=timediff(?,?) ");
		   
		     
		  //   EMPLOYEEID, TRANSACTIONDATE, TRANSACTIONTIME_IN, TRANSACTIONTIME_OUT, NETTIME, INOUT_STATUS, STATUS, LUPDATE, LASTUPDATE, MGRSTATUS;
		     
		     PreparedStatement stmt1=con.prepareStatement(INSERTQURY_2.toString());
		     
		      
		      rs = pstmt.executeQuery();
		     StringBuffer EmployeeId=new StringBuffer();
		     StringBuffer EmployeeIdMysql=new StringBuffer();
		 
		   while (rs.next()) {
		    
			   flag=true;
			   DATEON=rs.getString(1);
			   DAYTYPE=rs.getString("Daytype");
			   EmployeeId.append("'"+rs.getString("EmpCode")+"' ,");
			   EmployeeIdMysql.append(rs.getString("EmpCode")+",");
			   
			    empiid.add(rs.getString("EmpCode"));
			   
			    parrams.put(rs.getString("EmpCode")+"_AttendanceDate", ""+DATEON+"");
				parrams.put(rs.getString("EmpCode")+"_EmployeeName", rs.getString("EmpName"));
				parrams.put(rs.getString("EmpCode")+"_intime", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_outtime", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_workinghours", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_EmployeeCode", rs.getString("EmpCode"));
				parrams.put(rs.getString("EmpCode")+"_shiftFname", "NoShift");
				parrams.put(rs.getString("EmpCode")+"_mintime", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_mouttime", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_intime", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_outtime", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_EmployeeName", rs.getString("EmpName"));
				parrams.put(rs.getString("EmpCode")+"_shiftFname", "NoShift");
				parrams.put(rs.getString("EmpCode")+"_mintime", "00:00:00");
				parrams.put(rs.getString("EmpCode")+"_mouttime", "00:00:00");
				
				parrams.put(rs.getString("EmpCode")+"_DAYTYPE", DAYTYPE);
				
				parrams.put(rs.getString("EmpCode")+"_punchrecords", "NA");
				
				 Month=rs.getString("mnth");
		    	 Year=rs.getString("yer");
		    	 
				
			  /* stmt.setString(1, rs.getString(1));
			   stmt.setString(2, rs.getString(2));
			   stmt.setString(3, rs.getString(3));
			   stmt.setString(4, rs.getString(4));
			   
			   stmt.setString(5, rs.getString(1));
			   stmt.setString(6, rs.getString(2));
			   stmt.setString(7, rs.getString(3));
			   stmt.setString(8, rs.getString(4));*/
			  // DATEON=rs.getString(1);
		       /*stmt.addBatch();*/
		   }
	    EmployeeId.append("'0101010101'");
	    EmployeeIdMysql.append("0101010101");
		System.out.println(DATEON +" ~~~ " +EmployeeId);
		
		System.out.println(flag +" ::: flag");
		   
		   if(flag){ // Fetch Data From My sql Server
			   flag=false;
			  try{
			   Holidays.append(" select a.employeesequenceno,if(ifnull(b.holidaydate,'0000-00-00')='"+DATEON+"','HL', ");
			   Holidays.append(" if(dayname('"+DATEON+"')='Sunday','WOFF','WDAY' )) AS DAYTYPE ");
			   Holidays.append("  from hclhrm_prod.tbl_employee_primary A ");
			   Holidays.append(" left join hclhrm_prod.tbl_holidays B ON B.BUSINESSUNITID=a.companyid ");
			   Holidays.append(" where A.employeesequenceno in("+EmployeeIdMysql.toString()+") and B.holidaydate='"+DATEON+"' ");
			   Holidays.append(" and b.statusid=1001 ");
			   Holidays.append(" group by A.employeesequenceno ");
			   
			     pstmt=null;
			     rs=null;
			     pstmt = con.prepareStatement(Holidays.toString());
			     rs = pstmt.executeQuery();
			     while(rs.next()){
			    	 
			    	 parrams.put(rs.getString("employeesequenceno")+"_DAYTYPE", rs.getString("DAYTYPE"));
			    	
			    	 
			     }
			  }catch(Exception erd){
				  System.out.println("Exception erd : " +erd);
			  }
			   
			  // EmployeeId.append("'0101010101'");
			 //  MsSqlpstmt.setString(1, DATEON);
			//   MsSqlpstmt.setString(2, EmployeeId.toString());
			   
 /*Buffatt.append(" select  A.AttendanceDate ,B.EmployeeCode, B.EmployeeId , B.EmployeeName ,convert(varchar,CAST(A.InTime as datetime),8) as intime,convert(varchar,CAST(A.OutTime as datetime),8) as outtime , A.LateBy , A.EarlyBy , A.ShiftId,A.Duration ,convert(varchar(5),DateDiff(s, A.InTime, A.outTime)/3600)+':'+convert(varchar(5),DateDiff(s, A.InTime, A.outTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, A.InTime, A.outTime)%60)) workinghours,C.shiftFname,A.InTime as mintime,A.outTime mouttime,A.outDeviceid, A.punchrecords from dbo.AttendanceLogs  A ");
 Buffatt.append(" left join dbo.Employees  B ON B.employeeid=A.employeeid left join dbo.shifts C on  A.ShiftId=C.ShiftId ") ;
 Buffatt.append(" where A.AttendanceDate in('"+DATEON+"') and B.EmployeeCode in("+EmployeeId.toString()+") ");*/
	
 Buffatt.append(" select  A.AttendanceDate ,B.EmployeeCode, B.EmployeeId , B.EmployeeName ,convert(varchar,CAST(A.InTime as datetime),8) as intime,convert(varchar,CAST(A.OutTime as datetime),8) as outtime , A.LateBy , A.EarlyBy , A.ShiftId,A.Duration ,convert(varchar(5),DateDiff(s, A.InTime, A.outTime)/3600)+':'+convert(varchar(5),DateDiff(s, A.InTime, A.outTime)%3600/60)+':'+convert(varchar(5),(DateDiff(s, A.InTime, A.outTime)%60)) workinghours,C.shiftFname,A.InTime as mintime,A.outTime mouttime,A.outDeviceid, A.punchrecords from dbo.AttendanceLogs  A ");
 Buffatt.append(" left join dbo.Employees  B ON B.employeeid=A.employeeid left join dbo.shifts C on  A.ShiftId=C.ShiftId ") ;
 Buffatt.append(" where A.AttendanceDate in('"+DATEON+"') and B.EmployeeCode in("+EmployeeId.toString()+") ");
 
 
 System.out.println("Buffatt :: "+Buffatt.toString());
 
 //MsSqlpstmt = conSql.prepareStatement(Buffatt.toString());
			   
	     //   System.out.println("MsSqlpstmt ::" +MsSqlpstmt.toString());
			   rs=null;
			try{   
			   //rs = MsSqlpstmt.executeQuery();
		try{
			   rs = MsSqlpstmt.executeQuery(Buffatt.toString());
			   
		}catch(SQLException sql){
			
			System.out.println("sql Exception"+sql);
		}
			   
			   while (rs.next()) {
				    
				   
				//   System.out.println("Result Set Excecute Batch");
				   
				   flag=true;
				  // DATEON=rs.getString(1);
				 //  EmployeeId.append("'"+rs.getString("EmpCode")+"'");
				   
				  /* select  A.AttendanceDate , B.EmployeeCode, B.EmployeeId , B.EmployeeName 
				   ,convert(varchar,CAST(A.InTime as datetime),8) as intime,
				   convert(varchar,CAST(A.OutTime as datetime),8) as outtime ,
				    A.LateBy , A.EarlyBy , A.ShiftId,A.Duration ,
				    convert(varchar(5),
				    DateDiff(s, A.InTime, A.outTime)/3600)+':'+convert(varchar(5),
				    DateDiff(s, A.InTime, A.outTime)%3600/60)+':'+convert(varchar(5
				    DateDiff(s, A.InTime, A.outTime)%60)) 
				    workinghours from dbo.AttendanceLogs 
				    A left join dbo.Employees  B
				    ON B.employeeid=A.employeeid
				    where A.AttendanceDate in(?)
				    and B.EmployeeCode in(?);*/
				/*   insert into test_mum.tbl_att_leave_in_out_status_report
				   (DATEON,PAYPERIOD,EMPLOYEEID,NAME,ATT_IN,ATT_OUT,NET_HOURS,NET_FLEXI_HOURS,HOURS_DEDUCTION,DAYTYPE, DAY_STATUS)
				   values
				   (?,'201819',?,?,?,?,?,'0','0',0,'W','P')on duplicate key update 
				   ATT_IN=?, ATT_OUT=?,NET_HOURS=0,NAME=?,NET_FLEXI_HOURS=0,HOURS_DEDUCTION=0,DAYTYPE='W',DAY_STATUS=P,STATUS=1001;*/
				   /*Shifts
			         Shiftid
			         shiftname*/ 
			try{  
				
				String OUTDEVISEDID=null;
				try{
				OUTDEVISEDID=rs.getString("outDeviceid");
				}catch(Exception Erd){
					System.out.println("OUTDEVISEDID::" +Erd);
				}
				
				  
				
				parrams.put(rs.getString("EmployeeCode")+"_AttendanceDate", rs.getString("AttendanceDate"));
				parrams.put(rs.getString("EmployeeCode")+"_EmployeeName", rs.getString("EmployeeName"));
				
				parrams.put(rs.getString("EmployeeCode")+"_intime", rs.getString("intime"));
				if(OUTDEVISEDID!=null && OUTDEVISEDID.equalsIgnoreCase("SE")){//Swipe END
				 parrams.put(rs.getString("EmployeeCode")+"_outtime", rs.getString("intime"));
				}else{
					parrams.put(rs.getString("EmployeeCode")+"_outtime", rs.getString("outtime"));
				}
				
				if(OUTDEVISEDID!=null && OUTDEVISEDID.equalsIgnoreCase("SE")){//Swipe END
				 parrams.put(rs.getString("EmployeeCode")+"_workinghours", "00:00:00");
				}else{
					parrams.put(rs.getString("EmployeeCode")+"_workinghours", rs.getString("workinghours"));
				}
				
				
				parrams.put(rs.getString("EmployeeCode")+"_EmployeeCode", rs.getString("EmployeeCode"));
				parrams.put(rs.getString("EmployeeCode")+"_shiftFname", rs.getString("shiftFname"));
				
				parrams.put(rs.getString("EmployeeCode")+"_mintime", rs.getString("mintime"));
				
				if(OUTDEVISEDID!=null && OUTDEVISEDID.equalsIgnoreCase("SE")){//Swipe END
				parrams.put(rs.getString("EmployeeCode")+"_mouttime", rs.getString("mintime"));
				}else{
					parrams.put(rs.getString("EmployeeCode")+"_mouttime", rs.getString("mouttime"));
				}
				//parrams.put(rs.getString("EmployeeCode")+"_intime", rs.getString("intime"));
				//parrams.put(rs.getString("EmployeeCode")+"_outtime", rs.getString("outtime"));
				parrams.put(rs.getString("EmployeeCode")+"_EmployeeName", rs.getString("EmployeeName"));
				
				parrams.put(rs.getString("EmployeeCode")+"_punchrecords", rs.getString("punchrecords"));
				
				//parrams.put(rs.getString("EmployeeCode")+"_shiftFname", rs.getString("shiftFname"));
			//	parrams.put(rs.getString("EmployeeCode")+"_mintime", rs.getString("mintime"));
			//	parrams.put(rs.getString("EmployeeCode")+"_mouttime", rs.getString("mouttime"));
				
				/***********************
				   stmt.setString(1, rs.getString("AttendanceDate"));
				   stmt.setString(2, rs.getString("EmployeeCode"));
				   stmt.setString(3, rs.getString("EmployeeName"));
				   stmt.setString(4, rs.getString("intime"));
				   stmt.setString(5, rs.getString("outtime"));
				   stmt.setString(6, rs.getString("workinghours"));
				   
				   stmt.setString(7, rs.getString("shiftFname"));
				   
				   stmt.setString(8, rs.getString("mintime"));
				   stmt.setString(9, rs.getString("mouttime"));
				   
				   stmt.setString(10, rs.getString("intime"));
				   stmt.setString(11, rs.getString("outtime"));
				   stmt.setString(12, rs.getString("EmployeeName"));
				   
				   stmt.setString(13, rs.getString("shiftFname"));
				   stmt.setString(14, rs.getString("mintime"));
				   stmt.setString(15, rs.getString("mouttime"));
				   */
				  // System.out.println(rs.getString("shiftFname"));
				   
				  // DATEON=rs.getString(1);
			     //  stmt.addBatch();
			       
				 }catch(SQLException ert){
					 System.out.println("Exception Ert at insert:::"+ert);
				 }
			   }
			   
			}catch(Exception err){
				System.out.println("Exception at insert in MainTable"+ err);
			}
			   
			   
		   }
		  // flag=false;
		   
	Iterator ltr=empiid.iterator();
	if(flag){
		
		try{
		while(ltr.hasNext()){
			String Empid=ltr.next().toString();
			
			   stmt.setString(1, parrams.get(Empid+"_AttendanceDate").toString());
			   stmt.setString(2, parrams.get(Empid+"_EmployeeCode").toString());
			   stmt.setString(3, parrams.get(Empid+"_EmployeeName").toString());
			   stmt.setString(4, parrams.get(Empid+"_intime").toString());
			   stmt.setString(5, parrams.get(Empid+"_outtime").toString());
			   stmt.setString(6, parrams.get(Empid+"_workinghours").toString());
			   
			   stmt.setString(7, parrams.get(Empid+"_shiftFname").toString());
			   
			   stmt.setString(8, parrams.get(Empid+"_mintime").toString());
			   stmt.setString(9, parrams.get(Empid+"_mouttime").toString());
			   
			   stmt.setString(10, parrams.get(Empid+"_DAYTYPE").toString()); //Daytype
			   
			   String DayStatus="P";
			   if(parrams.get(Empid+"_intime").toString()!=null && parrams.get(Empid+"_outtime").toString()!=null && parrams.get(Empid+"_intime").toString().equalsIgnoreCase("00:00:00") && parrams.get(Empid+"_outtime").toString().equalsIgnoreCase("00:00:00"))
			   {
				   DayStatus="A";
			   }
			   stmt.setString(11, DayStatus);//daystatus
			   
			   stmt.setString(12, parrams.get(Empid+"_punchrecords").toString());
			   // CLOSE 
			   
			   // NEXT UPDATE for duplicate
			   
			   stmt.setString(13, parrams.get(Empid+"_intime").toString());
			   stmt.setString(14, parrams.get(Empid+"_outtime").toString());
			   stmt.setString(15, parrams.get(Empid+"_EmployeeName").toString());
			   
			   stmt.setString(16, parrams.get(Empid+"_shiftFname").toString());
			   stmt.setString(17, parrams.get(Empid+"_mintime").toString());
			   stmt.setString(18, parrams.get(Empid+"_mouttime").toString());
			   
			   stmt.setString(19, parrams.get(Empid+"_DAYTYPE").toString());//Daytype
			   stmt.setString(20, DayStatus);//daystatus
			   stmt.setString(21, parrams.get(Empid+"_workinghours").toString());//net hours
			   stmt.setString(22, parrams.get(Empid+"_punchrecords").toString());//net hours
			   
			  	   //rs = MsSqlpstmt.executeQuery();
				try{
					
					   String CalMnth=null;
					   StringBuffer Buffatt1=new StringBuffer();
					 //  System.out.println(CalMnth+ "Buffatt1::" +Buffatt1.toString());
					   CalMnth= Month+"_"+Year;
					 //  System.out.println(CalMnth+ "Buffatt2::" +Buffatt1.toString());
					//   Buffatt1.append(" select Userid,logDate,C1 from dbo.DeviceLogs_"+CalMnth+" where Userid in('"+Empid+"') and logDate between '"+parrams.get(Empid+"_mintime").toString()+"' and '"+parrams.get(Empid+"_mouttime").toString()+"'");
					
					   Buffatt1.append(" select Userid,logDate,C1 from dbo.DeviceLogs_"+CalMnth+" where Userid in('"+Empid+"') and logDate>'"+parrams.get(Empid+"_mintime").toString()+"' and logDate<'"+parrams.get(Empid+"_mouttime").toString()+"'");
					   // System.out.println(CalMnth+ "Buffatt3::" +Buffatt1.toString());
					   
					  String OUT_TIME_PRALLAL= parrams.get(Empid+"_mouttime").toString();
					   
					rs=null;
					rs = MsSqlpstmt1.executeQuery(Buffatt1.toString());
				String Insert_Type="FIRST",Insert_preview_value="0000-00-00 00:00:00";
					while (rs.next()) {
				
						
/*I
	/*	
						
	 INSERTQURY_2.append(" insert into procedure.tbl_employee_logs_allbu(EMPLOYEEID, TRANSACTIONDATE,"
	 		+ " TRANSACTIONTIME_IN,INOUT_STATUS,LASTUPDATE,TRANSACTIONTIME_OUT,NETTIME,STATUS)"
	 		+ " values (?,?,?,?,now(),?, timediff(?,?)) ");
	 INSERTQURY_2.append(" on duplicate key update EMPLOYEEID=?,"
	 		+ "TRANSACTIONDATE=?, TRANSACTIONTIME_IN=? ,"
	 		+ "INOUT_STATUS=?,LASTUPDATE=now(),"
	 		+ "TRANSACTIONTIME_OUT=?,NETTIME=timediff(?,?) ");
	 		*/
					   
					     
					     
						stmt1.setString(1, Empid);
						stmt1.setString(2, parrams.get(Empid+"_AttendanceDate").toString());
						stmt1.setString(3, rs.getString("logDate"));
						//stmt1.setString(4, rs.getString("C1"));
						
						String Data1=rs.getString("C1");
						String Data2="";
						
						String AttDate=parrams.get(Empid+"_AttendanceDate").toString();
						String FinalEnd=null;
						Insert_preview_value=rs.getString("logDate");
						//OUT_TIME_PRALLAL;
					try {
						if(rs.next()) {
							stmt1.setString(5, rs.getString("logDate"));
							FinalEnd=rs.getString("logDate");
							Data2=rs.getString("C1");
						}else {
							stmt1.setString(5, OUT_TIME_PRALLAL);
							FinalEnd=OUT_TIME_PRALLAL;
						}
						
					}catch(SQLException SQL) {
						System.out.println("SQL***:" +SQL.getMessage());
					}catch(Exception ER) {
						ER.printStackTrace();
					}
						
						stmt1.setString(4, Data1.concat(",").concat(Data2));
						//6,7 net time;
						
						stmt1.setString(6, FinalEnd );
						stmt1.setString(7, Insert_preview_value);
						
						stmt1.setString(8, Empid);
						stmt1.setString(9, AttDate);
						stmt1.setString(10,Insert_preview_value);
						stmt1.setString(11, Data1.concat(",").concat(Data2));
						stmt1.setString(12, FinalEnd);
						stmt1.setString(13, FinalEnd );
						stmt1.setString(14, Insert_preview_value);
						
						
						stmt1.addBatch();
					
						
					   }
					
				}catch(SQLException sql){
					
					System.out.println("sql Exception"+sql);
				}catch(Exception Erf) {
					
					System.out.println("Other Exp::"+Erf);
				}
					    
			   
			   stmt.addBatch();
			
		}
		}catch(Exception err){
			flag=false;
			System.out.println("Exception At AddBatch"+err);
		}
		
	}
		   if(flag){  
		   System.out.println("Excecute Batch");
		   excutBatch=stmt.executeBatch();
		  try {
		   stmt1.executeBatch();
		  }catch(Exception erd) {
			  erd.printStackTrace();
		  }
		   if(excutBatch.length>0){
		      map.put("exuFlag", "true");
		      if(CallNum==1){
		       map.put("DATEON", DATEON);
		      }
		   }else{
			   map.put("exuFlag", "false");
		   }
		   
		   }else{
			   
			    map.put("minLimit", "0");
				map.put("MaxLimit", "0");
				map.put("exuFlag", "false");
			   
		   }
		   
		}catch(Exception Err){
			System.out.println("Exception at sub class" +Err);
		}
	
		 return map;
	}
	
}
