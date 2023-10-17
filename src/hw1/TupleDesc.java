package hw1;
import java.util.*;

/*
 * 
 * HW2.TupDesc for Zhongyu Luo and Zhuobing Du
 * 
 * 
 */

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	
    	// Your code here
    	this.types = new Type[typeAr.length];
		this.fields = new String[fieldAr.length];

		for (int i = 0; i < typeAr.length; i++) {
			this.types[i] = typeAr[i];
			this.fields[i] = fieldAr[i];
		}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	
    	return this.fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	//your code here
    	if(i>=this.fields.length || i < 0) {
    		throw new NoSuchElementException("index " + i + " is not a valid field reference");
      }
    	return this.fields[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
    	//your code here
    	for (int i = 0; i < this.fields.length; i++) {
    		if(this.fields[i] == null ) {
    			return i;
    		}
    		else if (this.fields[i].equals(name)) {
    			return i;
    		}
    			}
    		throw new NoSuchElementException("No fueld with a matching name is found.");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
    	//your code here
    	if(i>=this.types.length || i < 0) {
    		throw new NoSuchElementException("index " + i + " is not a valid field reference");
    	}
    	return this.types[i];  
        
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	//your code here
    	int size = 0 ;
    	for (int i = 0; i < this.types.length; i++) {
    		if(this.types[i] == Type.INT) {
    			size = size + 4;
    		}
    		else {
    			size = size + 129;
    		}
    	}
    	return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	TupleDesc anotherT = (TupleDesc) o;
    	if (this.types.length != anotherT.types.length) {
    		return false;
    	}
    	else {
    		for(int i = 0; i < this.types.length; i++) {
    			if (!this.types[i].equals(anotherT.types[i])) {
    				return false;
    			}
    					
    		}
    		return true;
    	}
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
    	//your code here
    	StringBuilder decriptor = new StringBuilder();
    	for (int i = 0; i < this.types.length; i++) {
    		if(i > 0) {
    			decriptor.append(", ");
    		}
    		else {
    			decriptor.append(this.types[i].toString());
    			decriptor.append("(").append(this.fields[i]).append(")");
    		}
    	}
    	return decriptor.toString();
        
    }
}
