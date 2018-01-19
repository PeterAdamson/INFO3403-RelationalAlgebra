
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.apache.calcite.adapter.csv.CsvSchemaFactory; 
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author sray
 * INFO 3403, W2017
 * UNB
 *
 */

public class RelAlgebraAsgn {
	
	 private final boolean verbose= true;

	 Connection calConn = null;
	 
	 public static void main(String[] args) { 
		 new RelAlgebraAsgn().runAll();
	 }
	 
	//--------------------------------------------------------------------------------------
	 /**
	  * Create a relational algebra expression for the query:
      *
      * For each department, show the name of the department and 
      * the TOTAL salary earned by ALL employees in that department 
	  * 
	  */
	 private void asgn3_1(RelBuilder builder) {
		 System.out.println("Running: asgn3_1 ");
		 builder
		 .scan("EMPLOYEE").as("e")
		 .scan("DEPT").as("d")
		 .join(JoinRelType.INNER, "DEPTNO")
		 .aggregate(builder.groupKey(builder.field("d","NAME")),builder.sum(false,"SALARY", builder.field("SALARY")));
		 final RelNode node = builder.build();
		 if(verbose){
			 System.out.println(RelOptUtil.toString(node));
		 }
		 try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1) + " " + rs.getInt(2));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	 

	 /**
	  * Create a relational algebra expression for the query:
      * Show the name of the employees, the name of departments they work and the name of their managers
	  * 
	  */
	  private void asgn3_2(RelBuilder builder) {
		 System.out.println("Running: asgn3_2");
		 builder
		 .scan("EMPLOYEE").as("e")
		 .scan("DEPT").as("d")
		 .scan("EMPLOYEE").as("m")
		 .join(JoinRelType.INNER, "DEPTNO")
		 .join(JoinRelType.INNER)
		 .filter(builder.equals(builder.field("e","MGRID"),builder.field("m","EMPID")))
		 .project(builder.field("d","NAME"),builder.field("e","NAME"),builder.field("m","NAME"));
		 final RelNode node = builder.build();
		 if(verbose){
			 System.out.println(RelOptUtil.toString(node));
		 }
		 try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	  
	  /**
	   * Create a relational algebra expression for the query:
	   * Show the name of the manager who manages the most number of employees
	   * 
	   */
	  private void asgn3_3(RelBuilder builder) {
		  System.out.println("Running: asgn3_3");
		  builder
		  .scan("EMPLOYEE").as("e")
		  .scan("EMPLOYEE").as("m")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("e","MGRID"),builder.field("m","EMPID")))
		  .aggregate(builder.groupKey(builder.field("m","NAME")),builder.count(false, "C", builder.field("e","EMPID")))
		  .sort(builder.desc(builder.field("C")))
		  .limit(0,1);
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	  
	  
	  /**
	   * Create a relational algebra expression for the query:
	   *
	   * Show the title of the courses and the employees who have taken each course. Show the course
	   * course title even if no employee has taken it.
	   * 
	   */
	  private void asgn3_4(RelBuilder builder) {
		  System.out.println("Running: asgn3_4");			 
		  builder
		  .scan("EMPLOYEE").as("e")
		  .scan("CERTIFICATE").as("ce")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("e","EMPID"),builder.field("ce", "EMPID")))
		  .scan("COURSE").as("c")	  
		  .join(JoinRelType.LEFT)  
		  .filter(builder.equals(builder.field("c", "COURSEID"),builder.field("ce", "COURSEID")))
		  .project(builder.field("c","TITLE"),builder.field("e","NAME"));
		  /*final RelNode left = builder
				  .scan("EMPLOYEE")
				  .scan("CERTIFICATE")
				  .join(JoinRelType.INNER)
				  //.filter(builder.equals(builder.field("e","EMPID"),builder.field("ce", "EMPID")))
				  .project(builder.field("NAME"))
				  .build();
		  final RelNode right = builder
				  .scan("COURSE")
				  .scan("CERTIFICATE")
				  .join(JoinRelType.LEFT)
				  .project(builder.field("TITLE"))
				  .build();
		  builder
		  .push(right)
		  .push(left)
		  .join(JoinRelType.LEFT);*/
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 if(rs.getString(2) != null){
					 System.out.println(rs.getString(1) + " " + rs.getString(2));
				 }
				 else{
					 System.out.println(rs.getString(1)); 
				 }
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	  }
	  
	  
	  /**
	   * Create a relational algebra expression for the query:
	   *
	   * Show the name of the employees who have not taken any course.
	   * 
	   */
	  private void asgn3_5(RelBuilder builder) {
		  System.out.println("Running: asgn3_5");
		  final RelNode left = builder
				  .scan("EMPLOYEE")
				  .project(builder.field("EMPID"),builder.field("NAME"),builder.literal(0))
				  .build();
		  final RelNode right = builder
		  .scan("EMPLOYEE")
		  .scan("CERTIFICATE")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("EMPLOYEE", "EMPID"),builder.field("CERTIFICATE","EMPID")))
		  .project(builder.field("EMPID"),builder.field("NAME"),builder.literal(0))
		  .distinct()
		  .build();
		  builder
		  .push(left)
		  .push(right)
		  .minus(false)
		  .distinct();
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(2));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	  }
	  
	  
	 //--------------------------------------------------------------------------------------
	 
	
	 //---------------------------------------------------------------------------------------	
	 //---------------------------------------------------------------------------------------
	 public void runAll() {
		 // Create a builder. The config contains a schema mapped
		 final FrameworkConfig config = buildConfig();  
		 final RelBuilder builder = RelBuilder.create(config);
		 
		 for (int r = 0; r <= 7; r++) {
			 //runExample(builder, r);
		 }
		 	 
		 for (int i = 0; i <= 6; i++) {
			 runAssignmentTasks(builder, i);
		 }
		 
	 }

	 // Running the assignment 3 tasks
	 
	 private void runAssignmentTasks(RelBuilder builder, int i) {
	 
		 System.out.println("---------------------------------------------------------------------------------------");
		 switch (i) {
		 	 case 1:
		 		asgn3_1(builder);
		 		 break;
		 	case 2:
		 		asgn3_2(builder);
		 		 break;
		 	case 3:
		 		asgn3_3(builder);
		 		 break;
		 	case 4:
		 		asgn3_4(builder);
		 		 break;
		 	case 5:
		 		asgn3_5(builder);
		 		 break;
		 }
	 }
	 
	 // Running the examples
	 private void runExample(RelBuilder builder, int i) {
		 System.out.println("---------------------------------------------------------------------------------------");
		 switch (i) {
			 case 0:
				 example0(builder);
				 break;
			 case 1:
				 example1(builder);
				 break;
			 case 2:
				 example2(builder);
				 break;
			 case 3:
				 example3(builder);
				 break;
			 case 4:
				 example4(builder); 
				 break;
			 case 5:
				 example5(builder);
				 break;
			 case 6:
				 example6(builder);
				 break;
			 case 7:
				 example7(builder);
				 break;
			 default:
				 throw new AssertionError("unknown example " + i);
		 }
	 }

	//---------------------------------------------------------------------------------------
	

	 /**
	  * TABLE SCAN
	  * Creates a relational algebra expression for the query:
	  * Running: Show the details of the courses
	  */
	 private void example0(RelBuilder builder) {
		 System.err.println("Running: select * from COURSE");
		 builder
		 .scan("COURSE");
			  
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
			  
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();
			 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 
	 /**
	  * PROJECTION
	  * Creates a relational algebra expression for the query:
	  * Show the title of the course where courseid = 2
	  */
	 private void example1(RelBuilder builder) {
		 System.err.println("\nRunning: Show the title of the course where courseid = 2");
		 builder
		 .scan("COURSE")
		 .filter(  builder.equals(builder.field("COURSEID"), builder.literal(2))  )
		 // or
		 //.filter(  builder.call(SqlStdOperatorTable.EQUALS, builder.field("COURSEID"), builder.literal(2) ) )
		 .project(builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getString(1));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 /**
	  * SELECTION
	  * Creates a relational algebra expression for the query:
	  * Show the details of the courses where courseid > 2
	  */
	 private void example2(RelBuilder builder) {
		 System.err.println("\nRunning: Show the details of the courses where courseid > 2");
		 builder
		 .scan("COURSE")
		 .filter(  builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("COURSEID"), builder.literal(2) )  
				 )
		 .project(builder.field("COURSEID"), builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 
	 /**
	  * ORDER BY
	  * Creates a relational algebra expression for the query:
	  * Show the details of the first 5 courses sorted by the course [in descending order] [offset 2 limit 5]
	  */
	 private void example3(RelBuilder builder) {
		 System.err.println("\nRunning: Select * from COURSE order by COURSEID limit 5");
		 builder
		 .scan("COURSE")
		 .sort(  builder.field("COURSEID")  )  
		 //.sort( builder.desc( builder.field("COURSEID"))  )    // in descending order
		 .limit(2, 3) // offset 2, limit 3
		 .project(builder.field("COURSEID"), builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 

	 /**
	  * GROUP BY HAVING
	  * Creates a relational algebra expression for the query:
	  * Show the number of courses in each course category [ where number of courses is greater than 1]
	  *
	  * SELECT CATID, count(*) AS C, 
	  * FROM COURSE
	  * GROUP BY CATID
	  * HAVING C > 1
	  */
	 private void example4(RelBuilder builder) {
		 System.err.println("\nRunning: Show the number of courses in each course category where number of courses is greater than 1");
		 builder
		 .scan("COURSE")
		 .aggregate(builder.groupKey("CATEGORYID"),
		            // builder.count(false, "C")
				    // or
				    builder.count(false, "C", builder.field("COURSEID") )
		            )
		 .filter( builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"), builder.literal(1)));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getInt(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 /**
	  * UNION/INTERSECT/MINUS
	  * 
	  * NOTE:
	  * There is a bug which may cause the set operations not execute the query properly:
	  * Bug: CompileException on UNION ALL query when result only contains one column (https://issues.apache.org/jira/browse/KYLIN-2200)
	  * 
	  * To have Calcite generate the relational algebra expression without throwing an exception
	  * add a dummy literal as a second column, if you have projected only one column.
	  * This will help generate he relational algebra expression properly. But the "query" may still not execute correctly.
	  * 
	  * 
	  * Creates a relational algebra expression for the query:
	  * Show all categories from COURSE and CCATEGORY
	  *
	  * SELECT CATEGORYID FROM COURSE 
	  * Union
	  * SELECT CATID from CCATEGORY
	  */
	 private void example5(RelBuilder builder) {
		 System.err.println("\nRunning: Show all categories from COURSE and CCATEGORY");
		 builder
		 .scan("COURSE").project(builder.field("CATEGORYID"), builder.literal(0)) //Add a dummy literal 
		 .scan("CCATEGORY").project(builder.field("CATID"), builder.literal(0)) //Add a dummy literal
		 .union(false);
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTr
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.apache.calcite.adapter.csv.CsvSchemaFactory; 
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author sray
 * INFO 3403, W2017
 * UNB
 *
 */

public class RelAlgebraAsgn {
	
	 private final boolean verbose= true;

	 Connection calConn = null;
	 
	 public static void main(String[] args) { 
		 new RelAlgebraAsgn().runAll();
	 }
	 
	//--------------------------------------------------------------------------------------
	 /**
	  * Create a relational algebra expression for the query:
      *
      * For each department, show the name of the department and 
      * the TOTAL salary earned by ALL employees in that department 
	  * 
	  */
	 private void asgn3_1(RelBuilder builder) {
		 System.out.println("Running: asgn3_1 ");
		 builder
		 .scan("EMPLOYEE").as("e")
		 .scan("DEPT").as("d")
		 .join(JoinRelType.INNER, "DEPTNO")
		 .aggregate(builder.groupKey(builder.field("d","NAME")),builder.sum(false,"SALARY", builder.field("SALARY")));
		 final RelNode node = builder.build();
		 if(verbose){
			 System.out.println(RelOptUtil.toString(node));
		 }
		 try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1) + " " + rs.getInt(2));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	 

	 /**
	  * Create a relational algebra expression for the query:
      * Show the name of the employees, the name of departments they work and the name of their managers
	  * 
	  */
	  private void asgn3_2(RelBuilder builder) {
		 System.out.println("Running: asgn3_2");
		 builder
		 .scan("EMPLOYEE").as("e")
		 .scan("DEPT").as("d")
		 .scan("EMPLOYEE").as("m")
		 .join(JoinRelType.INNER, "DEPTNO")
		 .join(JoinRelType.INNER)
		 .filter(builder.equals(builder.field("e","MGRID"),builder.field("m","EMPID")))
		 .project(builder.field("d","NAME"),builder.field("e","NAME"),builder.field("m","NAME"));
		 final RelNode node = builder.build();
		 if(verbose){
			 System.out.println(RelOptUtil.toString(node));
		 }
		 try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	  
	  /**
	   * Create a relational algebra expression for the query:
	   * Show the name of the manager who manages the most number of employees
	   * 
	   */
	  private void asgn3_3(RelBuilder builder) {
		  System.out.println("Running: asgn3_3");
		  builder
		  .scan("EMPLOYEE").as("e")
		  .scan("EMPLOYEE").as("m")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("e","MGRID"),builder.field("m","EMPID")))
		  .aggregate(builder.groupKey(builder.field("m","NAME")),builder.count(false, "C", builder.field("e","EMPID")))
		  .sort(builder.desc(builder.field("C")))
		  .limit(0,1);
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	  
	  
	  /**
	   * Create a relational algebra expression for the query:
	   *
	   * Show the title of the courses and the employees who have taken each course. Show the course
	   * course title even if no employee has taken it.
	   * 
	   */
	  private void asgn3_4(RelBuilder builder) {
		  System.out.println("Running: asgn3_4");			 
		  builder
		  .scan("EMPLOYEE").as("e")
		  .scan("CERTIFICATE").as("ce")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("e","EMPID"),builder.field("ce", "EMPID")))
		  .scan("COURSE").as("c")	  
		  .join(JoinRelType.LEFT)  
		  .filter(builder.equals(builder.field("c", "COURSEID"),builder.field("ce", "COURSEID")))
		  .project(builder.field("c","TITLE"),builder.field("e","NAME"));
		  /*final RelNode left = builder
				  .scan("EMPLOYEE")
				  .scan("CERTIFICATE")
				  .join(JoinRelType.INNER)
				  //.filter(builder.equals(builder.field("e","EMPID"),builder.field("ce", "EMPID")))
				  .project(builder.field("NAME"))
				  .build();
		  final RelNode right = builder
				  .scan("COURSE")
				  .scan("CERTIFICATE")
				  .join(JoinRelType.LEFT)
				  .project(builder.field("TITLE"))
				  .build();
		  builder
		  .push(right)
		  .push(left)
		  .join(JoinRelType.LEFT);*/
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 if(rs.getString(2) != null){
					 System.out.println(rs.getString(1) + " " + rs.getString(2));
				 }
				 else{
					 System.out.println(rs.getString(1)); 
				 }
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	  }
	  
	  
	  /**
	   * Create a relational algebra expression for the query:
	   *
	   * Show the name of the employees who have not taken any course.
	   * 
	   */
	  private void asgn3_5(RelBuilder builder) {
		  System.out.println("Running: asgn3_5");
		  final RelNode left = builder
				  .scan("EMPLOYEE")
				  .project(builder.field("EMPID"),builder.field("NAME"),builder.literal(0))
				  .build();
		  final RelNode right = builder
		  .scan("EMPLOYEE")
		  .scan("CERTIFICATE")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("EMPLOYEE", "EMPID"),builder.field("CERTIFICATE","EMPID")))
		  .project(builder.field("EMPID"),builder.field("NAME"),builder.literal(0))
		  .distinct()
		  .build();
		  builder
		  .push(left)
		  .push(right)
		  .minus(false)
		  .distinct();
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(2));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	  }
	  
	  
	 //--------------------------------------------------------------------------------------
	 
	
	 //---------------------------------------------------------------------------------------	
	 //---------------------------------------------------------------------------------------
	 public void runAll() {
		 // Create a builder. The config contains a schema mapped
		 final FrameworkConfig config = buildConfig();  
		 final RelBuilder builder = RelBuilder.create(config);
		 
		 for (int r = 0; r <= 7; r++) {
			 //runExample(builder, r);
		 }
		 	 
		 for (int i = 0; i <= 6; i++) {
			 runAssignmentTasks(builder, i);
		 }
		 
	 }

	 // Running the assignment 3 tasks
	 
	 private void runAssignmentTasks(RelBuilder builder, int i) {
	 
		 System.out.println("---------------------------------------------------------------------------------------");
		 switch (i) {
		 	 case 1:
		 		asgn3_1(builder);
		 		 break;
		 	case 2:
		 		asgn3_2(builder);
		 		 break;
		 	case 3:
		 		asgn3_3(builder);
		 		 break;
		 	case 4:
		 		asgn3_4(builder);
		 		 break;
		 	case 5:
		 		asgn3_5(builder);
		 		 break;
		 }
	 }
	 
	 // Running the examples
	 private void runExample(RelBuilder builder, int i) {
		 System.out.println("---------------------------------------------------------------------------------------");
		 switch (i) {
			 case 0:
				 example0(builder);
				 break;
			 case 1:
				 example1(builder);
				 break;
			 case 2:
				 example2(builder);
				 break;
			 case 3:
				 example3(builder);
				 break;
			 case 4:
				 example4(builder); 
				 break;
			 case 5:
				 example5(builder);
				 break;
			 case 6:
				 example6(builder);
				 break;
			 case 7:
				 example7(builder);
				 break;
			 default:
				 throw new AssertionError("unknown example " + i);
		 }
	 }

	//---------------------------------------------------------------------------------------
	

	 /**
	  * TABLE SCAN
	  * Creates a relational algebra expression for the query:
	  * Running: Show the details of the courses
	  */
	 private void example0(RelBuilder builder) {
		 System.err.println("Running: select * from COURSE");
		 builder
		 .scan("COURSE");
			  
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
			  
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();
			 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 
	 /**
	  * PROJECTION
	  * Creates a relational algebra expression for the query:
	  * Show the title of the course where courseid = 2
	  */
	 private void example1(RelBuilder builder) {
		 System.err.println("\nRunning: Show the title of the course where courseid = 2");
		 builder
		 .scan("COURSE")
		 .filter(  builder.equals(builder.field("COURSEID"), builder.literal(2))  )
		 // or
		 //.filter(  builder.call(SqlStdOperatorTable.EQUALS, builder.field("COURSEID"), builder.literal(2) ) )
		 .project(builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getString(1));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 /**
	  * SELECTION
	  * Creates a relational algebra expression for the query:
	  * Show the details of the courses where courseid > 2
	  */
	 private void example2(RelBuilder builder) {
		 System.err.println("\nRunning: Show the details of the courses where courseid > 2");
		 builder
		 .scan("COURSE")
		 .filter(  builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("COURSEID"), builder.literal(2) )  
				 )
		 .project(builder.field("COURSEID"), builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 
	 /**
	  * ORDER BY
	  * Creates a relational algebra expression for the query:
	  * Show the details of the first 5 courses sorted by the course [in descending order] [offset 2 limit 5]
	  */
	 private void example3(RelBuilder builder) {
		 System.err.println("\nRunning: Select * from COURSE order by COURSEID limit 5");
		 builder
		 .scan("COURSE")
		 .sort(  builder.field("COURSEID")  )  
		 //.sort( builder.desc( builder.field("COURSEID"))  )    // in descending order
		 .limit(2, 3) // offset 2, limit 3
		 .project(builder.field("COURSEID"), builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 

	 /**
	  * GROUP BY HAVING
	  * Creates a relational algebra expression for the query:
	  * Show the number of courses in each course category [ where number of courses is greater than 1]
	  *
	  * SELECT CATID, count(*) AS C, 
	  * FROM COURSE
	  * GROUP BY CATID
	  * HAVING C > 1
	  */
	 private void example4(RelBuilder builder) {
		 System.err.println("\nRunning: Show the number of courses in each course category where number of courses is greater than 1");
		 builder
		 .scan("COURSE")
		 .aggregate(builder.groupKey("CATEGORYID"),
		            // builder.count(false, "C")
				    // or
				    builder.count(false, "C", builder.field("COURSEID") )
		            )
		 .filter( builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"), builder.literal(1)));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getInt(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 /**
	  * UNION/INTERSECT/MINUS
	  * 
	  * NOTE:
	  * There is a bug which may cause the set operations not execute the query properly:
	  * Bug: CompileException on UNION ALL query when result only contains one column (https://issues.apache.org/jira/browse/KYLIN-2200)
	  * 
	  * To have Calcite generate the relational algebra expression without throwing an exception
	  * add a dummy literal as a second column, if you have projected only one column.
	  * This will help generate he relational algebra expression properly. But the "query" may still not execute correctly.
	  * 
	  * 
	  * Creates a relational algebra expression for the query:
	  * Show all categories from COURSE and CCATEGORY
	  *
	  * SELECT CATEGORYID FROM COURSE 
	  * Union
	  * SELECT CATID from CCATEGORY
	  */
	 private void example5(RelBuilder builder) {
		 System.err.println("\nRunning: Show all categories from COURSE and CCATEGORY");
		 builder
		 .scan("COURSE").project(builder.field("CATEGORYID"), builder.literal(0)) //Add a dummy literal 
		 .scan("CCATEGORY").project(builder.field("CATID"), builder.literal(0)) //Add a dummy literal
		 .union(false);
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 /**
	  * CROSS PRODUCT
	  * Creates a relational algebra expression for the query:
	  *
	  * SELECT * FROM COURSE, CCATEGORY
	  */
	 private void example6(RelBuilder builder) {
		 System.err.println("\nRunning: SELECT * FROM COURSE, CCATEGORY");
		 builder
		 .scan("COURSE")
		 .scan("CCATEGORY")
		 .join(JoinRelType.INNER);
		
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2)+ " " +rs.getInt(3) + " " + rs.getInt(4)+ " " + rs.getString(5));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 
	 /**
	  * INNER JOIN
	  * Creates a relational algebra expression for the query:
	  * Show the title of each course along with the name of the course category
	  *
	  * SELECT TITLE, CATNAME
	  * FROM COURSE c, CCATEGORY g
	  * WHERE c.CATEGORYID = g.CATID

	  */
	 private void example7(RelBuilder builder) {
		 System.err.println("\nRunning: Show the title of each course along with the name of the course category");
		 builder
		 .scan("COURSE").as("c")
		 .scan("CCATEGORY").as("g")
		 .join(JoinRelType.INNER)
		 .filter( builder.equals(builder.field("c", "CATEGORYID"), builder.field("g", "CATID")))
		 // Syntax:.filter (predicate1, predicate2);  where "," implies AND
		 .project(builder.field("TITLE"), builder.field("CATNAME"));
		 
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getString(1)+ " -> " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 /**
	  * Sometimes the stack becomes so deeply nested it gets confusing. To keep
	  * things straight, you can remove expressions from the stack. For example,
	  * here we are building a bushy join:
	  *
	  * <pre>
	  *                join
	  *              /      \
	  *         join          join
	  *       /      \      /      \
	  * CUSTOMERS ORDERS LINE_ITEMS PRODUCTS
	  * </pre>
	  *
	  * <p>We build it in three stages. Store the intermediate results in variables
	  * `left` and `right`, and use `push()` to put them back on the stack when it
	  * is time to create the final `Join`.
	  */
	 private void example8(RelBuilder builder) {  
		 System.out.println("Running exampleDoesNotRun");
		 final RelNode left = builder
		        .scan("CUSTOMERS")
		        .scan("ORDERS")
		        .join(JoinRelType.INNER, "ORDER_ID")
		        .build();

		 final RelNode right = builder
		        .scan("LINE_ITEMS")
		        .scan("PRODUCTS")
		        .join(JoinRelType.INNER, "PRODUCT_ID")
		        .build();

		 builder
		        .push(left)
		        .push(right)
		        .join(JoinRelType.INNER, "ORDER_ID");
		     
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
	 }
		 
	 // setting all up
		  
	 private String jsonPath(String model) {
		 return resourcePath(model + ".json");
	 }

	 private String resourcePath(String path) {
		 final URL url = RelAlgebraAsgn.class.getResource("/resources/" + path);
			 
		 String s = url.toString();
		 if (s.startsWith("file:")) {
			 s = s.substring("file:".length());
		 }
		 return s;
	 }
		  
	 private FrameworkConfig  buildConfig() {
		 FrameworkConfig calciteFrameworkConfig= null;
			  
		 Connection connection = null;
		 Statement statement = null;
		 try {
			 Properties info = new Properties();
			 info.put("model", jsonPath("datamodel"));
			 connection = DriverManager.getConnection("jdbc:calcite:", info);
			      
			 final CalciteConnection calciteConnection = connection.unwrap(
			              CalciteConnection.class);

			 calConn = calciteConnection;
			 SchemaPlus rootSchemaPlus = calciteConnection.getRootSchema();
			      
			 final Schema schema =
			              CsvSchemaFactory.INSTANCE
			                  .create(rootSchemaPlus, null,
			                      ImmutableMap.<String, Object>of("directory",
			                          resourcePath("company"), "flavor", "scannable"));
			      

			 SchemaPlus companySchema = rootSchemaPlus.getSubSchema("company");
			    		  
			      
			 System.out.println("Available tables in the database:");
			 Set<String>  tables=rootSchemaPlus.getSubSchema("company").getTableNames();
			 for (String t: tables)
				 System.out.println(t);
			      
			     
			 final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

			 traitDefs.add(ConventionTraitDef.INSTANCE);
			 traitDefs.add(RelCollationTraitDef.INSTANCE);

			 calciteFrameworkConfig = Frameworks.newConfigBuilder()
			          .parserConfig(SqlParser.configBuilder()
			              // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
			              // case when they are read, and whether identifiers are matched case-sensitively.
			              .setLex(Lex.MYSQL)
			              .build())
			          // Sets the schema to use by the planner
			          .defaultSchema(companySchema) 
			          .traitDefs(traitDefs)
			          // Context provides a way to store data within the planner session that can be accessed in planner rules.
			          .context(Contexts.EMPTY_CONTEXT)
			          // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
			          .ruleSets(RuleSets.ofList())
			          // Custom cost factory to use during optimization
			          .costFactory(null)
			          .typeSystem(RelDataTypeSystem.DEFAULT)
			          .build();
			     
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
		 return calciteFrameworkConfig;
	 }
}
ace();
		 }
	 }
	 
	 
	 /**
	  * CROSS PRODUCT
	  * Creates a relational algebra expression for the query:
	  *
	  * SELECT * FROM COURSE, CCATEGORY
	  */
	 private void example6(RelBuilder builder) {
		 System.err.println("\nRunning: SELECT * FROM COURSE, CCATEGORY");
		 builder
		 .scan("COURSE")
		 .scan("CCATEGORY")
		 .join(JoinRelType.INNER);
		
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2)+ " " +rs.getInt(3) + " " + rs.getInt(4)+ " " + rs.getString(5));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 
	 /**
	  * INNER JOIN
	  * Creates a relational algebra expression for the query:
	  * Show the title of each course along with the name of the course category
	  *
	  * SELECT TITLE, CATNAME
	  * FROM COURSE c, CCATEGORY g
	  * WHERE c.CATEGORYID = g.CATID

	  */
	 private void example7(RelBuilder builder) {
		 System.err.println("\nRunning: Show the title of each course along with the name of the course category");
		 builder
		 .scan("COURSE").as("c")
		 .scan("CCATEGORY").as("g")
		 .join(JoinRelType.INNER)
		 .filter( builder.equals(builder.field("c", "CATEGORYID"), builder.field("g", "CATID")))
		 // Syntax:.filter (predicate1, predicate2);  where "," implies AND
		 .project(builder.field("TITLE"), builder.field("CATNAME"));
		 
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getString(1)+ " -> " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 /**
	  * Sometimes the stack becomes so deeply nested it gets confusing. To keep
	  * things straight, you can remove expressions from the stack. For example,
	  * here we are building a bushy join:
	  *
	  * <pre>
	  *                join
	  *              /      \
	  *         join          join
	  *       /      \      /      \
	  * CUSTOMERS ORDERS LINE_ITEMS PRODUCTS
	  * </pre>
	  *
	  * <p>We build it in three stages. Store the intermediate results in variables
	  * `left` and `right`, and use `push()` to put them back on the stack when it
	  * is time to create the final `Join`.
	  */
	 private void example8(RelBuilder builder) {  
		 System.out.println("Running exampleDoesNotRun");
		 final RelNode left = builder
		        .scan("CUSTOMERS")
		        .scan("ORDERS")
		        .join(JoinRelType.INNER, "ORDER_ID")
		        .build();

		 final RelNode right = builder
		        .scan("LINE_ITEMS")
		        .scan("PRODUCTS")
		        .join(JoinRelType.INNER, "PRODUCT_ID")
		        .build();

		 builder
		        .push(left)
		        .push(right)
		        .join(JoinRelType.INNER, "ORDER_ID");
		     
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
	 }
		 
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.apache.calcite.adapter.csv.CsvSchemaFactory; 
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author sray
 * INFO 3403, W2017
 * UNB
 *
 */

public class RelAlgebraAsgn {
	
	 private final boolean verbose= true;

	 Connection calConn = null;
	 
	 public static void main(String[] args) { 
		 new RelAlgebraAsgn().runAll();
	 }
	 
	//--------------------------------------------------------------------------------------
	 /**
	  * Create a relational algebra expression for the query:
      *
      * For each department, show the name of the department and 
      * the TOTAL salary earned by ALL employees in that department 
	  * 
	  */
	 private void asgn3_1(RelBuilder builder) {
		 System.out.println("Running: asgn3_1 ");
		 builder
		 .scan("EMPLOYEE").as("e")
		 .scan("DEPT").as("d")
		 .join(JoinRelType.INNER, "DEPTNO")
		 .aggregate(builder.groupKey(builder.field("d","NAME")),builder.sum(false,"SALARY", builder.field("SALARY")));
		 final RelNode node = builder.build();
		 if(verbose){
			 System.out.println(RelOptUtil.toString(node));
		 }
		 try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1) + " " + rs.getInt(2));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	 

	 /**
	  * Create a relational algebra expression for the query:
      * Show the name of the employees, the name of departments they work and the name of their managers
	  * 
	  */
	  private void asgn3_2(RelBuilder builder) {
		 System.out.println("Running: asgn3_2");
		 builder
		 .scan("EMPLOYEE").as("e")
		 .scan("DEPT").as("d")
		 .scan("EMPLOYEE").as("m")
		 .join(JoinRelType.INNER, "DEPTNO")
		 .join(JoinRelType.INNER)
		 .filter(builder.equals(builder.field("e","MGRID"),builder.field("m","EMPID")))
		 .project(builder.field("d","NAME"),builder.field("e","NAME"),builder.field("m","NAME"));
		 final RelNode node = builder.build();
		 if(verbose){
			 System.out.println(RelOptUtil.toString(node));
		 }
		 try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	  
	  /**
	   * Create a relational algebra expression for the query:
	   * Show the name of the manager who manages the most number of employees
	   * 
	   */
	  private void asgn3_3(RelBuilder builder) {
		  System.out.println("Running: asgn3_3");
		  builder
		  .scan("EMPLOYEE").as("e")
		  .scan("EMPLOYEE").as("m")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("e","MGRID"),builder.field("m","EMPID")))
		  .aggregate(builder.groupKey(builder.field("m","NAME")),builder.count(false, "C", builder.field("e","EMPID")))
		  .sort(builder.desc(builder.field("C")))
		  .limit(0,1);
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(1));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	 }
	  
	  
	  /**
	   * Create a relational algebra expression for the query:
	   *
	   * Show the title of the courses and the employees who have taken each course. Show the course
	   * course title even if no employee has taken it.
	   * 
	   */
	  private void asgn3_4(RelBuilder builder) {
		  System.out.println("Running: asgn3_4");			 
		  builder
		  .scan("EMPLOYEE").as("e")
		  .scan("CERTIFICATE").as("ce")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("e","EMPID"),builder.field("ce", "EMPID")))
		  .scan("COURSE").as("c")	  
		  .join(JoinRelType.LEFT)  
		  .filter(builder.equals(builder.field("c", "COURSEID"),builder.field("ce", "COURSEID")))
		  .project(builder.field("c","TITLE"),builder.field("e","NAME"));
		  /*final RelNode left = builder
				  .scan("EMPLOYEE")
				  .scan("CERTIFICATE")
				  .join(JoinRelType.INNER)
				  //.filter(builder.equals(builder.field("e","EMPID"),builder.field("ce", "EMPID")))
				  .project(builder.field("NAME"))
				  .build();
		  final RelNode right = builder
				  .scan("COURSE")
				  .scan("CERTIFICATE")
				  .join(JoinRelType.LEFT)
				  .project(builder.field("TITLE"))
				  .build();
		  builder
		  .push(right)
		  .push(left)
		  .join(JoinRelType.LEFT);*/
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 if(rs.getString(2) != null){
					 System.out.println(rs.getString(1) + " " + rs.getString(2));
				 }
				 else{
					 System.out.println(rs.getString(1)); 
				 }
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	  }
	  
	  
	  /**
	   * Create a relational algebra expression for the query:
	   *
	   * Show the name of the employees who have not taken any course.
	   * 
	   */
	  private void asgn3_5(RelBuilder builder) {
		  System.out.println("Running: asgn3_5");
		  final RelNode left = builder
				  .scan("EMPLOYEE")
				  .project(builder.field("EMPID"),builder.field("NAME"),builder.literal(0))
				  .build();
		  final RelNode right = builder
		  .scan("EMPLOYEE")
		  .scan("CERTIFICATE")
		  .join(JoinRelType.INNER)
		  .filter(builder.equals(builder.field("EMPLOYEE", "EMPID"),builder.field("CERTIFICATE","EMPID")))
		  .project(builder.field("EMPID"),builder.field("NAME"),builder.literal(0))
		  .distinct()
		  .build();
		  builder
		  .push(left)
		  .push(right)
		  .minus(false)
		  .distinct();
		  final RelNode node = builder.build();
	      if(verbose){
			System.out.println(RelOptUtil.toString(node));
		  }
		  try{
			 final PreparedStatement preparedStatement = RelRunners.run(node,calConn);
			 ResultSet rs = preparedStatement.executeQuery();
			 while(rs.next()){
				 System.out.println(rs.getString(2));
			 }
			 rs.close();
		 } catch (SQLException e){
			 e.printStackTrace();
		 }
	  }
	  
	  
	 //--------------------------------------------------------------------------------------
	 
	
	 //---------------------------------------------------------------------------------------	
	 //---------------------------------------------------------------------------------------
	 public void runAll() {
		 // Create a builder. The config contains a schema mapped
		 final FrameworkConfig config = buildConfig();  
		 final RelBuilder builder = RelBuilder.create(config);
		 
		 for (int r = 0; r <= 7; r++) {
			 //runExample(builder, r);
		 }
		 	 
		 for (int i = 0; i <= 6; i++) {
			 runAssignmentTasks(builder, i);
		 }
		 
	 }

	 // Running the assignment 3 tasks
	 
	 private void runAssignmentTasks(RelBuilder builder, int i) {
	 
		 System.out.println("---------------------------------------------------------------------------------------");
		 switch (i) {
		 	 case 1:
		 		asgn3_1(builder);
		 		 break;
		 	case 2:
		 		asgn3_2(builder);
		 		 break;
		 	case 3:
		 		asgn3_3(builder);
		 		 break;
		 	case 4:
		 		asgn3_4(builder);
		 		 break;
		 	case 5:
		 		asgn3_5(builder);
		 		 break;
		 }
	 }
	 
	 // Running the examples
	 private void runExample(RelBuilder builder, int i) {
		 System.out.println("---------------------------------------------------------------------------------------");
		 switch (i) {
			 case 0:
				 example0(builder);
				 break;
			 case 1:
				 example1(builder);
				 break;
			 case 2:
				 example2(builder);
				 break;
			 case 3:
				 example3(builder);
				 break;
			 case 4:
				 example4(builder); 
				 break;
			 case 5:
				 example5(builder);
				 break;
			 case 6:
				 example6(builder);
				 break;
			 case 7:
				 example7(builder);
				 break;
			 default:
				 throw new AssertionError("unknown example " + i);
		 }
	 }

	//---------------------------------------------------------------------------------------
	

	 /**
	  * TABLE SCAN
	  * Creates a relational algebra expression for the query:
	  * Running: Show the details of the courses
	  */
	 private void example0(RelBuilder builder) {
		 System.err.println("Running: select * from COURSE");
		 builder
		 .scan("COURSE");
			  
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
			  
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();
			 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 
	 /**
	  * PROJECTION
	  * Creates a relational algebra expression for the query:
	  * Show the title of the course where courseid = 2
	  */
	 private void example1(RelBuilder builder) {
		 System.err.println("\nRunning: Show the title of the course where courseid = 2");
		 builder
		 .scan("COURSE")
		 .filter(  builder.equals(builder.field("COURSEID"), builder.literal(2))  )
		 // or
		 //.filter(  builder.call(SqlStdOperatorTable.EQUALS, builder.field("COURSEID"), builder.literal(2) ) )
		 .project(builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getString(1));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 /**
	  * SELECTION
	  * Creates a relational algebra expression for the query:
	  * Show the details of the courses where courseid > 2
	  */
	 private void example2(RelBuilder builder) {
		 System.err.println("\nRunning: Show the details of the courses where courseid > 2");
		 builder
		 .scan("COURSE")
		 .filter(  builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("COURSEID"), builder.literal(2) )  
				 )
		 .project(builder.field("COURSEID"), builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 
	 /**
	  * ORDER BY
	  * Creates a relational algebra expression for the query:
	  * Show the details of the first 5 courses sorted by the course [in descending order] [offset 2 limit 5]
	  */
	 private void example3(RelBuilder builder) {
		 System.err.println("\nRunning: Select * from COURSE order by COURSEID limit 5");
		 builder
		 .scan("COURSE")
		 .sort(  builder.field("COURSEID")  )  
		 //.sort( builder.desc( builder.field("COURSEID"))  )    // in descending order
		 .limit(2, 3) // offset 2, limit 3
		 .project(builder.field("COURSEID"), builder.field("TITLE"));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 

	 /**
	  * GROUP BY HAVING
	  * Creates a relational algebra expression for the query:
	  * Show the number of courses in each course category [ where number of courses is greater than 1]
	  *
	  * SELECT CATID, count(*) AS C, 
	  * FROM COURSE
	  * GROUP BY CATID
	  * HAVING C > 1
	  */
	 private void example4(RelBuilder builder) {
		 System.err.println("\nRunning: Show the number of courses in each course category where number of courses is greater than 1");
		 builder
		 .scan("COURSE")
		 .aggregate(builder.groupKey("CATEGORYID"),
		            // builder.count(false, "C")
				    // or
				    builder.count(false, "C", builder.field("COURSEID") )
		            )
		 .filter( builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"), builder.literal(1)));
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getInt(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }

	 /**
	  * UNION/INTERSECT/MINUS
	  * 
	  * NOTE:
	  * There is a bug which may cause the set operations not execute the query properly:
	  * Bug: CompileException on UNION ALL query when result only contains one column (https://issues.apache.org/jira/browse/KYLIN-2200)
	  * 
	  * To have Calcite generate the relational algebra expression without throwing an exception
	  * add a dummy literal as a second column, if you have projected only one column.
	  * This will help generate he relational algebra expression properly. But the "query" may still not execute correctly.
	  * 
	  * 
	  * Creates a relational algebra expression for the query:
	  * Show all categories from COURSE and CCATEGORY
	  *
	  * SELECT CATEGORYID FROM COURSE 
	  * Union
	  * SELECT CATID from CCATEGORY
	  */
	 private void example5(RelBuilder builder) {
		 System.err.println("\nRunning: Show all categories from COURSE and CCATEGORY");
		 builder
		 .scan("COURSE").project(builder.field("CATEGORYID"), builder.literal(0)) //Add a dummy literal 
		 .scan("CCATEGORY").project(builder.field("CATID"), builder.literal(0)) //Add a dummy literal
		 .union(false);
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 /**
	  * CROSS PRODUCT
	  * Creates a relational algebra expression for the query:
	  *
	  * SELECT * FROM COURSE, CCATEGORY
	  */
	 private void example6(RelBuilder builder) {
		 System.err.println("\nRunning: SELECT * FROM COURSE, CCATEGORY");
		 builder
		 .scan("COURSE")
		 .scan("CCATEGORY")
		 .join(JoinRelType.INNER);
		
		    
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getInt(1)+ " " + rs.getString(2)+ " " +rs.getInt(3) + " " + rs.getInt(4)+ " " + rs.getString(5));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 
	 /**
	  * INNER JOIN
	  * Creates a relational algebra expression for the query:
	  * Show the title of each course along with the name of the course category
	  *
	  * SELECT TITLE, CATNAME
	  * FROM COURSE c, CCATEGORY g
	  * WHERE c.CATEGORYID = g.CATID

	  */
	 private void example7(RelBuilder builder) {
		 System.err.println("\nRunning: Show the title of each course along with the name of the course category");
		 builder
		 .scan("COURSE").as("c")
		 .scan("CCATEGORY").as("g")
		 .join(JoinRelType.INNER)
		 .filter( builder.equals(builder.field("c", "CATEGORYID"), builder.field("g", "CATID")))
		 // Syntax:.filter (predicate1, predicate2);  where "," implies AND
		 .project(builder.field("TITLE"), builder.field("CATNAME"));
		 
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
		    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 while (rs.next()) {
				 System.out.println(rs.getString(1)+ " -> " + rs.getString(2));
			 }
			 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }
	 
	 
	 /**
	  * Sometimes the stack becomes so deeply nested it gets confusing. To keep
	  * things straight, you can remove expressions from the stack. For example,
	  * here we are building a bushy join:
	  *
	  * <pre>
	  *                join
	  *              /      \
	  *         join          join
	  *       /      \      /      \
	  * CUSTOMERS ORDERS LINE_ITEMS PRODUCTS
	  * </pre>
	  *
	  * <p>We build it in three stages. Store the intermediate results in variables
	  * `left` and `right`, and use `push()` to put them back on the stack when it
	  * is time to create the final `Join`.
	  */
	 private void example8(RelBuilder builder) {  
		 System.out.println("Running exampleDoesNotRun");
		 final RelNode left = builder
		        .scan("CUSTOMERS")
		        .scan("ORDERS")
		        .join(JoinRelType.INNER, "ORDER_ID")
		        .build();

		 final RelNode right = builder
		        .scan("LINE_ITEMS")
		        .scan("PRODUCTS")
		        .join(JoinRelType.INNER, "PRODUCT_ID")
		        .build();

		 builder
		        .push(left)
		        .push(right)
		        .join(JoinRelType.INNER, "ORDER_ID");
		     
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
	 }
		 
	 // setting all up
		  
	 private String jsonPath(String model) {
		 return resourcePath(model + ".json");
	 }

	 private String resourcePath(String path) {
		 final URL url = RelAlgebraAsgn.class.getResource("/resources/" + path);
			 
		 String s = url.toString();
		 if (s.startsWith("file:")) {
			 s = s.substring("file:".length());
		 }
		 return s;
	 }
		  
	 private FrameworkConfig  buildConfig() {
		 FrameworkConfig calciteFrameworkConfig= null;
			  
		 Connection connection = null;
		 Statement statement = null;
		 try {
			 Properties info = new Properties();
			 info.put("model", jsonPath("datamodel"));
			 connection = DriverManager.getConnection("jdbc:calcite:", info);
			      
			 final CalciteConnection calciteConnection = connection.unwrap(
			              CalciteConnection.class);

			 calConn = calciteConnection;
			 SchemaPlus rootSchemaPlus = calciteConnection.getRootSchema();
			      
			 final Schema schema =
			              CsvSchemaFactory.INSTANCE
			                  .create(rootSchemaPlus, null,
			                      ImmutableMap.<String, Object>of("directory",
			                          resourcePath("company"), "flavor", "scannable"));
			      

			 SchemaPlus companySchema = rootSchemaPlus.getSubSchema("company");
			    		  
			      
			 System.out.println("Available tables in the database:");
			 Set<String>  tables=rootSchemaPlus.getSubSchema("company").getTableNames();
			 for (String t: tables)
				 System.out.println(t);
			      
			     
			 final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

			 traitDefs.add(ConventionTraitDef.INSTANCE);
			 traitDefs.add(RelCollationTraitDef.INSTANCE);

			 calciteFrameworkConfig = Frameworks.newConfigBuilder()
			          .parserConfig(SqlParser.configBuilder()
			              // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
			              // case when they are read, and whether identifiers are matched case-sensitively.
			              .setLex(Lex.MYSQL)
			              .build())
			          // Sets the schema to use by the planner
			          .defaultSchema(companySchema) 
			          .traitDefs(traitDefs)
			          // Context provides a way to store data within the planner session that can be accessed in planner rules.
			          .context(Contexts.EMPTY_CONTEXT)
			          // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
			          .ruleSets(RuleSets.ofList())
			          // Custom cost factory to use during optimization
			          .costFactory(null)
			          .typeSystem(RelDataTypeSystem.DEFAULT)
			          .build();
			     
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
		 return calciteFrameworkConfig;
	 }
}

	 // setting all up
		  
	 private String jsonPath(String model) {
		 return resourcePath(model + ".json");
	 }

	 private String resourcePath(String path) {
		 final URL url = RelAlgebraAsgn.class.getResource("/resources/" + path);
			 
		 String s = url.toString();
		 if (s.startsWith("file:")) {
			 s = s.substring("file:".length());
		 }
		 return s;
	 }
		  
	 private FrameworkConfig  buildConfig() {
		 FrameworkConfig calciteFrameworkConfig= null;
			  
		 Connection connection = null;
		 Statement statement = null;
		 try {
			 Properties info = new Properties();
			 info.put("model", jsonPath("datamodel"));
			 connection = DriverManager.getConnection("jdbc:calcite:", info);
			      
			 final CalciteConnection calciteConnection = connection.unwrap(
			              CalciteConnection.class);

			 calConn = calciteConnection;
			 SchemaPlus rootSchemaPlus = calciteConnection.getRootSchema();
			      
			 final Schema schema =
			              CsvSchemaFactory.INSTANCE
			                  .create(rootSchemaPlus, null,
			                      ImmutableMap.<String, Object>of("directory",
			                          resourcePath("company"), "flavor", "scannable"));
			      

			 SchemaPlus companySchema = rootSchemaPlus.getSubSchema("company");
			    		  
			      
			 System.out.println("Available tables in the database:");
			 Set<String>  tables=rootSchemaPlus.getSubSchema("company").getTableNames();
			 for (String t: tables)
				 System.out.println(t);
			      
			     
			 final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

			 traitDefs.add(ConventionTraitDef.INSTANCE);
			 traitDefs.add(RelCollationTraitDef.INSTANCE);

			 calciteFrameworkConfig = Frameworks.newConfigBuilder()
			          .parserConfig(SqlParser.configBuilder()
			              // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
			              // case when they are read, and whether identifiers are matched case-sensitively.
			              .setLex(Lex.MYSQL)
			              .build())
			          // Sets the schema to use by the planner
			          .defaultSchema(companySchema) 
			          .traitDefs(traitDefs)
			          // Context provides a way to store data within the planner session that can be accessed in planner rules.
			          .context(Contexts.EMPTY_CONTEXT)
			          // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
			          .ruleSets(RuleSets.ofList())
			          // Custom cost factory to use during optimization
			          .costFactory(null)
			          .typeSystem(RelDataTypeSystem.DEFAULT)
			          .build();
			     
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
		 return calciteFrameworkConfig;
	 }
}
