package hw1;
/*
 * 
 * HW2. Relation for Zhongyu Luo and Zhuobing Du
 * 
 * 
 */

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> tp, TupleDesc td) {
		//your code here
		tuples = tp;
		
		
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> resultTuples = new ArrayList<>();
		
		
		for (Tuple tP : tuples) {
			
			if (tP.getField(field).compare(op, operand)) {
				
				resultTuples.add(tP);
			}
		}
		this.tuples = resultTuples;
		 
		return this;
		
	}
	
	
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names)throws Exception {
		//your code here
		int c_Num = this.td.numFields();
		
		
		
		String[] c_name = new String[c_Num];
		
		
		
		
		Type[] td_Types = new Type[c_Num];
	
		for (int i = 0; i < c_Num; i++) {
			
			c_name[i] = td.getFieldName(i);
			td_Types[i] = td.getType(i);
		}
		for (String st : c_name) {
			
			if (names.contains(st)) {
				
				throw new Exception();
			}
			
		}
		
	
		// Name Change
		for (int i = 0; i < fields.size(); i++) {
			
			int FieldIdToChange = fields.get(i);
			
			
			if (names.get(i) == null || names.get(i).length() == 0) continue;
			
			c_name[FieldIdToChange] = names.get(i);
		}
		TupleDesc newTupleDesc = new TupleDesc(td_Types, c_name);
		
		this.td = newTupleDesc;
		
		for (Tuple tuple : tuples) {
			
			tuple.setDesc(newTupleDesc);
			
		}
		
		return this;
		
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
	
		int c_Num = fields.size();
	
		String[] c_Name = new String[c_Num];
		Type[] td_NTypes = new Type[c_Num];
		
		
		for (int i = 0; i < c_Num; i++) {
			
			int field_Num = fields.get(i);
			
			c_Name[i] = td.getFieldName(field_Num);
			
			td_NTypes[i] = td.getType(field_Num);
		}
		TupleDesc n_TP = new TupleDesc(td_NTypes, c_Name);
		
		
		/*
		 * Here we can generate the picked data(Tuples) 
		 * 
		 */
		
		
		// To deal with the 0 case 
		if (fields.size() == 0) return new Relation(new ArrayList<>(), n_TP);
		
		
	
		
		for (Tuple oldTuple : tuples) {
			oldTuple.changeFieldToProject(fields);
			oldTuple.setDesc(n_TP);
		}
		this.td = n_TP;
		
		
	
		return this;
		
		
	}
	
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		
		int new_field_num = this.td.numFields() + other.getDesc().numFields();
		String[] new_cName = new String[new_field_num];
				
				
		Type[] td_NewTypes = new Type[new_field_num];
				
		for (int i = 0; i < this.td.numFields(); i++) {
					
			new_cName[i] = this.td.getFieldName(i);
					
					
			td_NewTypes[i] = this.td.getType(i);
		}
		for (int i = td.numFields(); i < new_field_num; i++) {
			
			int j = i - td.numFields();
			new_cName[i] = other.getDesc().getFieldName(j);
			td_NewTypes[i] = other.getDesc().getType(j);
			
		}
		
		
		TupleDesc n_TP = new TupleDesc(td_NewTypes, new_cName);
				
	
		ArrayList<Tuple> final_Result = new ArrayList<>();
		
		
		for (Tuple thisTuple : tuples) {
			
			for (Tuple otherTuple : other.getTuples()) {
				
				
				if (thisTuple.getField(field1).compare(RelationalOperator.EQ, otherTuple.getField(field2))) {
					
					Tuple newTuple = new Tuple(n_TP);
					
					for (int i = 0; i < thisTuple.getDesc().numFields(); i++) {
						
						newTuple.setField(i, thisTuple.getField(i));
					}
					for (int i = 0; i < otherTuple.getDesc().numFields(); i++) {
						
						newTuple.setField(i + thisTuple.getDesc().numFields(), otherTuple.getField(i));
					}
					
					final_Result.add(newTuple);
				}
			}
		}
		
		
		this.tuples = final_Result;
		
		
		
		this.td = n_TP;		
		return this;
	
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		
		
		Aggregator aggregator = new Aggregator(op, groupBy, td);
		
		for (Tuple tP : tuples) {
			
			aggregator.merge(tP);
			
		}
		
		ArrayList<Tuple> temp = aggregator.getResults();
		
		return new Relation(temp, temp.get(0).getDesc());
		
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		
		//Initiate a String Builder to accomadate the String 
		//So that it it mutable 
		StringBuilder new_Sb = new StringBuilder("");
		
		
		new_Sb.append(this.td.toString());
		new_Sb.append("\n");
		
		
		for (Tuple tuple : tuples) {
			
			
			new_Sb.append(tuple.toString());
			new_Sb.append("\n");
		}
		return new_Sb.toString();
	}
}
