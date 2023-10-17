package hw1;
/*
 * 
 * HW2. HeapFile for Zhongyu Luo and Zhuobing Du
 * 
 * 
 */


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	private File file;
	private TupleDesc type;
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.file = f;
		this.type = type;
	}
	
	public File getFile() {
		//your code here
		return this.file;
		
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.type;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		byte[] data = new byte[PAGE_SIZE];
		try(RandomAccessFile rea = new RandomAccessFile(file,"r")) {
			rea.seek(PAGE_SIZE*id);
			rea.readFully(data);
			int tableId = this.getId(); 
			return new HeapPage(id,data,tableId);
		}catch(IOException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.file.getAbsoluteFile().hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		byte[] data = p.getPageData();
		try(RandomAccessFile rea = new RandomAccessFile(this.file,"rw")){
			rea.seek(PAGE_SIZE*p.getId());
			rea.write(data);
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		int num = this.getNumPages();
	    for(int i = 0; i < num; i++) {
	        HeapPage p = this.readPage(i);
	        if(p.isnotfull()) {
	            try {
	                p.addTuple(t);

	      
	                this.writePage(p); 

	                return p;
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }
	    }

	    return null;
		
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		int pid = t.getPid();
		HeapPage p = readPage(pid);
		p.deleteTuple(t);
		this.writePage(p);
		
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> allt = new ArrayList<>();
		int np = this.getNumPages();
		for(int i = 0; i  <np; i++) {
			HeapPage p = this.readPage(i);
			Iterator<Tuple> ite = p.iterator();
			while(ite.hasNext()) {
				allt.add(ite.next());
			}
		}
		return allt;
		
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return (int)(this.file.length()/PAGE_SIZE);
		
	}
}
