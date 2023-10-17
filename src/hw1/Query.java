package hw1;
/*
 * 
 * HW2.Query for Zhongyu Luo and Zhuobing Du
 * 
 * 
 */

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
		

		// Create the  table 
		Table que_Table = (Table) sb.getFromItem();
		
		// Get the catalog 
		Catalog catalog = Database.getCatalog();
		
		// Get table name and table id 
		String que_Table_Name = que_Table.getName();
		int que_Table_ID = catalog.getTableId(que_Table_Name);
		
		
		//Get the Tuples and catalog 
		HeapFile que_Table_HeapFile = catalog.getDbFile(que_Table_ID);
		ArrayList<Tuple> que_Tuples = que_Table_HeapFile.getAllTuples();
		
		// Description 
		TupleDesc que_Tuple_Desc = que_Table_HeapFile.getTupleDesc();
		
		// Crate the Relation 
		Relation que_Relation = new Relation(que_Tuples, que_Tuple_Desc);
		
		
		
		// Use the library to apply the WHERE
		
		Expression WHERE = sb.getWhere();
		if (WHERE != null) {
			WhereExpressionVisitor w_Visitor = new WhereExpressionVisitor();
			WHERE.accept(w_Visitor);
			
			// Apply the conditions 
			String w_Left_Name = w_Visitor.getLeft();
			Field right_Column = w_Visitor.getRight();
			
			que_Relation = que_Relation.select(que_Relation.getDesc().nameToId(w_Left_Name),
					w_Visitor.getOp(), right_Column);

		}
		
		

		
		// Use the library to apply the JOIN 
		List<Join> JOIN = sb.getJoins();
		
		if (JOIN != null) {
			
			
			
			for (Join join : JOIN) {
				
				
				Table j_Table = (Table) join.getRightItem();
				
				
				String j_TableName = j_Table.getName();
				int j_TableId = catalog.getTableId(j_TableName);
				TupleDesc j_Desc = catalog.getTupleDesc(j_TableId);
				ArrayList<Tuple> j_Tuples = catalog.getDbFile(j_TableId).getAllTuples();
				
				// Create the new relation for the result
				Relation j_Relation = new Relation(j_Tuples, j_Desc);

				// Use the library to apply the JOIN ON 
				String[] j_Table_Columns = join.getOnExpression().toString().split("=");
				
				// The column name to JOIN for left and right table
				String[] j_Left_Name = j_Table_Columns[0].trim().split("\\.");
				String[] j_Right_Name = j_Table_Columns[1].trim().split("\\.");
				
				// Pick the left and right name 
				String left_Name = j_Left_Name[1];
				String right_Name = j_Right_Name[1];

				if (!j_TableName.toLowerCase().equals(j_Right_Name[0].toLowerCase())) {
					/*
					 * Judge if the key(name) of the column names are equal
					 * If so, merge them
					 */
					String temp = left_Name;
					left_Name = right_Name;
					right_Name = temp;
				}
				
				
				int left_ID = que_Relation.getDesc().nameToId(left_Name);
				int right_ID = j_Relation.getDesc().nameToId(right_Name);
				
				// Get the result of JOIN 
				que_Relation = que_Relation.join(j_Relation, left_ID, right_ID);

			}

		}
		
		
		
		// Use the library to apply the PROJECT
		
		List<SelectItem> SELECT = sb.getSelectItems();
		ArrayList<Integer> project_Column_ID = new ArrayList<>(); 
		
		
		ColumnVisitor c_Visitor = new ColumnVisitor();

		for (SelectItem item : SELECT) {
			
			item.accept(c_Visitor);
			
			// Judge if the select column is Aggregated 
			String selectCol = c_Visitor.isAggregate() ? item.toString() : c_Visitor.getColumn(); 
			if (selectCol.equals("*")) {
			
				for (int i = 0; i < que_Relation.getDesc().numFields(); i++) {
					project_Column_ID.add(i);
				}
				break;
			}
			int column_ID = c_Visitor.getColumn().equals("*") && c_Visitor.isAggregate() ? 0: que_Relation.getDesc().nameToId(c_Visitor.getColumn());
			if (!project_Column_ID.contains(column_ID)) project_Column_ID.add(column_ID);
			
		}
		
		
		
		// Use the library to apply the SELECT Aggregate
		
		if (c_Visitor.isAggregate()) {
			if (sb.getGroupByColumnReferences() == null) {
				que_Relation = que_Relation.project(project_Column_ID);
				return que_Relation.aggregate(c_Visitor.getOp(), false);
			}
			
			que_Relation = que_Relation.aggregate(c_Visitor.getOp(), sb.getGroupByColumnReferences() != null);
			
		}
		que_Relation = que_Relation.project(project_Column_ID);
		

		return que_Relation;
		
		
	}
}
