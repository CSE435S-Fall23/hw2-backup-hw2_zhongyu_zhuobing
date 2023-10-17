package hw1;

/*
 * 
 * HW2.Tuple for Zhongyu Luo and Zhuobing Du
 * 
 * 
 */

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	private TupleDesc tupledecs;
	private int page_id;
	private Field[] fields;
	private int slot_id;
	public Tuple(TupleDesc t) {
		//your code here
		this.tupledecs = t;
		this.fields = new Field[t.numFields()];
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.tupledecs;
		
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return this.page_id;
			
	}

	public void setPid(int pid) {
		//your code here
		this.page_id = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return this.slot_id;
		
	}

	public void setId(int id) {
		//your code here
		this.slot_id = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.tupledecs =td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		//your code here
		this.fields[i] = v;
	}
	
	public Field getField(int i) {
		//your code here
		return this.fields[i];
		
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		StringBuilder repre = new StringBuilder();
		for(int i = 0; i < this.fields.length; i++) {
			if(i == 0) {
				repre.append("(");
			}
			repre.append(this.tupledecs.getFieldName(i)).append(": ").append(this.fields.toString());
			if (i == this.fields.length - 1) {
				repre.append(")");
			}
		}
		return repre.toString();
		
	}
	
	public void changeFieldToProject(ArrayList<Integer> fields) {
		Field[] newFields = new Field[fields.size()];
		for (int i = 0; i < fields.size(); i++) {
			newFields[i] = getField(fields.get(i));
		}
		this.fields = newFields;
	}
}
	