package hw1;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/*
 * 
 * HW2. Aggregator for Zhongyu Luo and Zhuobing Du
 * 
 * 
 */


/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	// This ori_tuple is used to stored the input tuples 
	private ArrayList<Tuple> ori_Tuple;
	
	// Define a HashMap for the result of the tuples after groupby
	private HashMap<Field, ArrayList<Tuple>> gb_Tuple;
	
	// Tuple Description 
	private TupleDesc td;
	
	// Aggregate method 
	private AggregateOperator op;
	
	// If apply groupby 
	private boolean gb_Need;
	


	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		
		//Constructor 
		ori_Tuple = new ArrayList<>();
		gb_Tuple = new HashMap<>();
		
		
		this.op = o;
		this.gb_Need = groupBy;
		this.td = td;

	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		switch (this.op) {
		
		case MAX:
			if (this.gb_Need) {
				gb_Tuple_Generator(t);
			} else {
				
				if (ori_Tuple.isEmpty()) {
					ori_Tuple.add(t);
				} else {
					Tuple oldMaxTuple = ori_Tuple.get(0);

					if (t.getField(0).compare(RelationalOperator.GT, oldMaxTuple.getField(0))) {
						ori_Tuple.set(0, t);

					}
				}
			}
			break;

			
		case MIN:
			if (this.gb_Need) {
				gb_Tuple_Generator(t);
			} else {
				
				if (ori_Tuple.isEmpty()) {
					ori_Tuple.add(t);
				} else {
					Tuple oldTupleMinTuple = ori_Tuple.get(0);
					if (t.getField(0).compare(RelationalOperator.LT, oldTupleMinTuple.getField(0))) {
						ori_Tuple.set(0, t);

					}
				}
				
			}
			break;
			

		case AVG:
			if (this.gb_Need) {
				gb_Tuple_Generator(t);
			} else {
				ori_Tuple.add(t);
			}
			break;

		case COUNT:
			if (this.gb_Need) {
				gb_Tuple_Generator(t);
			} else {
				ori_Tuple.add(t);
			}
			break;

		case SUM:
			if (this.gb_Need) {
				gb_Tuple_Generator(t);
			} else {
				ori_Tuple.add(t);
			}
			break;

		default:
			throw new IllegalArgumentException("Illegal AggregateOperator.");
		}
	}
	
	private void gb_Tuple_Generator(Tuple t) {
		
		
		/*
		 * Generate the groupby needed tuple,
		 * The gb_Tuple would have two fields:
		 * 	1, The group the passed in tuple belongs to 
		 * 	2, The corresponding data  
		 */

		Field key = t.getField(0);
		if (gb_Tuple.containsKey(key)) 
			
			gb_Tuple.get(key).add(t);
			
		 else {
			 
			gb_Tuple.put(key, new ArrayList<>());
			gb_Tuple.get(key).add(t);

		}

	}
	
	
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		switch (this.op) {
		
		case MAX:
			if (this.gb_Need) {
				
				ArrayList<Tuple> resultArrayList = new ArrayList<>();

				for (Map.Entry<Field, ArrayList<Tuple>> eachGroupEntry : gb_Tuple.entrySet()) {
					ArrayList<Tuple> eachGroupList = eachGroupEntry.getValue();

					Tuple currentMaxTuple = eachGroupList.get(0);
					for (Tuple tupleInThisGroupTuple : eachGroupList) {
		
						if (tupleInThisGroupTuple.getField(1).compare(RelationalOperator.GT, currentMaxTuple.getField(1))) {
							currentMaxTuple = tupleInThisGroupTuple;
						}
					}
					resultArrayList.add(currentMaxTuple);
				}

				return resultArrayList;
					
			} else {

			}
			break;

		case MIN:
			if (this.gb_Need) {
				ArrayList<Tuple> resultArrayList = new ArrayList<>();
				if (!gb_Tuple.isEmpty()) {
					for (Map.Entry<Field, ArrayList<Tuple>> eachGroupEntry : gb_Tuple.entrySet()) {
						ArrayList<Tuple> eachGroupList = eachGroupEntry.getValue();

						Tuple currentMinTuple = eachGroupList.get(0);
						for (Tuple tupleInThisGroupTuple : eachGroupList) {
							
							if (tupleInThisGroupTuple.getField(1).compare(RelationalOperator.LT, currentMinTuple.getField(1))) {
								currentMinTuple = tupleInThisGroupTuple;
							}
						}
						resultArrayList.add(currentMinTuple);
					}

				}
				return resultArrayList;
				
			} else {

			}
			break;
			

		case AVG:
			if (this.gb_Need) {
				if (td.getType(1) == Type.STRING)
					return null; 
				ArrayList<Tuple> resutlArrayList = new ArrayList<>();
				
				Type[] tupesTypes = new Type[] { Type.INT, Type.INT };
				String[] nameStrings = new String[] { td.getFieldName(0), "AVG" };
				TupleDesc newTupleDesc = new TupleDesc(tupesTypes, nameStrings);

				for (Map.Entry<Field, ArrayList<Tuple>> eachEntry : gb_Tuple.entrySet()) {
					ArrayList<Tuple> eachGrouList = eachEntry.getValue();

					Field keyField = eachEntry.getKey();

					int numTupleInGroup = eachGrouList.size();
					int sum = 0;
					for (Tuple tupleInThisGroup : eachGrouList) {
						sum += tupleInThisGroup.getField(1).hashCode();
					}
					
					int avgForThisGroup = sum / numTupleInGroup;
					Tuple newTuple = new Tuple(newTupleDesc);
					newTuple.setField(0, keyField);
					newTuple.setField(1, new IntField(avgForThisGroup));
					resutlArrayList.add(newTuple);

				}
				return resutlArrayList;

			} else {
				ArrayList<Tuple> resultArrayList = new ArrayList<>();
				if (td.getType(0) == Type.STRING)
					return null;
				int totalNum = ori_Tuple.size();
				int sum = 0;
				for (Tuple tuple : ori_Tuple) {
					sum += tuple.getField(0).hashCode();
				}
				int avg = sum / totalNum;
				Type[] tupesTypes = new Type[] { Type.INT };
				String[] nameStrings = new String[] { "AVG" };
				TupleDesc newTupleDesc = new TupleDesc(tupesTypes, nameStrings);
				Tuple newTuple = new Tuple(newTupleDesc);
				newTuple.setField(0, new IntField(avg));
				resultArrayList.add(newTuple);

				return resultArrayList;

			}

			
		case COUNT:
			if (this.gb_Need) {
				Type[] tupesTypes = new Type[] { this.td.getType(0), Type.INT };
				String[] nameStrings = new String[] { td.getFieldName(0), "COUNT" };
				TupleDesc newTupleDesc = new TupleDesc(tupesTypes, nameStrings);

				ArrayList<Tuple> resultArrayList = new ArrayList<>();

				for (Map.Entry<Field, ArrayList<Tuple>> eachGroupEntry : gb_Tuple.entrySet()) {
					ArrayList<Tuple> eachGroupList = eachGroupEntry.getValue();
					Field keyField = eachGroupEntry.getKey();
					int count = eachGroupList.size();
					Tuple newTuple = new Tuple(newTupleDesc);
					newTuple.setField(0, keyField);
					newTuple.setField(1, new IntField(count));
					resultArrayList.add(newTuple);

				}
				return resultArrayList;

			} else {
				Type[] tupesTypes = new Type[] { Type.INT };
				String[] nameStrings = new String[] { "COUNT" };
				TupleDesc newTupleDesc = new TupleDesc(tupesTypes, nameStrings);

				ArrayList<Tuple> resultArrayList = new ArrayList<>();
				Tuple newTuple = new Tuple(newTupleDesc);
				newTuple.setField(0, new IntField(ori_Tuple.size()));
				resultArrayList.add(newTuple);
				return resultArrayList;
				
			}

		case SUM:
			if (this.gb_Need) {
				
				ArrayList<Tuple> resultArrayList = new ArrayList<>();
				Type[] tupesTypes = new Type[] { this.td.getType(0), Type.INT };
				String[] nameStrings = new String[] { td.getFieldName(0), "SUM" };
				TupleDesc newTupleDesc = new TupleDesc(tupesTypes, nameStrings);
				if (td.getType(1) == Type.STRING)
					return null;
				for (Map.Entry<Field, ArrayList<Tuple>> eachGroupEntry : gb_Tuple.entrySet()) {
					ArrayList<Tuple> eachGroupList = eachGroupEntry.getValue();
					Field keyField = eachGroupEntry.getKey();
					int sumGroup = 0;
					for (Tuple tupleInThisGroupTuple : eachGroupList) {
						sumGroup += tupleInThisGroupTuple.getField(1).hashCode();
					}
					Tuple newTuple = new Tuple(newTupleDesc);
					newTuple.setField(0, keyField);
					newTuple.setField(1, new IntField(sumGroup));
					resultArrayList.add(newTuple);
					
				}

				return resultArrayList;

			} else {
				ArrayList<Tuple> resultArrayList = new ArrayList<>();
				Type[] tupesTypes = new Type[] { Type.INT };
				String[] nameStrings = new String[] { "SUM" };
				TupleDesc newTupleDesc = new TupleDesc(tupesTypes, nameStrings);
				if (td.getType(0) == Type.STRING)
					return null;
				int sum = 0;
				for (Tuple tuple : ori_Tuple) {
					sum += tuple.getField(0).hashCode();
				}
				Tuple newTuple = new Tuple(newTupleDesc);
				newTuple.setField(0, new IntField(sum));
				resultArrayList.add(newTuple);
				return resultArrayList;

			}
			

		default:
			throw new IllegalArgumentException("Unexpected value: " + this.op);
		}

		return this.ori_Tuple;
	}
}
