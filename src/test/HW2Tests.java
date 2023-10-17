package test;
/*
 * 
 * HW2.self_Unit_Test  for Zhongyu Luo and Zhuobing Du
 * 
 * 
 */

/*
 * 
 *In this self test, we decide to test  whether the query would work 
 *
 *We'll test the SELECT FROM WHERE 
 *
 *and the Rename
 * 
 */
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.IntField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Query;
import hw1.Relation;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;


public class HW2Tests {
	
	

		private HeapFile testhf;
		private TupleDesc testtd;
		private HeapFile ahf;
		private TupleDesc atd;
		private Catalog c;

		@Before
		public void setup() {
			
			try {
				Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
				Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				System.out.println("unable to copy files");
				e.printStackTrace();
			}
			
			c = Database.getCatalog();
			c.loadSchema("testfiles/test.txt");
			
			int tableId = c.getTableId("test");
			testtd = c.getTupleDesc(tableId);
			testhf = c.getDbFile(tableId);
			
			c = Database.getCatalog();
			c.loadSchema("testfiles/A.txt");
			
			tableId = c.getTableId("A");
			atd = c.getTupleDesc(tableId);
			ahf = c.getDbFile(tableId);
			System.out.println(new Relation(ahf.getAllTuples(), atd));
			System.out.println(new Relation(testhf.getAllTuples(), atd));
		}
	
		@Test
		/*
		 * This is the test for the SELECT WHERE FROM
		 * 
		 */
		
		public void testSFW() {
			
			/*
			 * We'd like to find  if the result is correct
			 * 
			 * When we select a2 where a1=200
			 * 
			 */
			Query que = new Query("SELECT a2 FROM A WHERE a1 = 200");
			Relation re_1 = que.execute();
			System.out.println();
			
			
			ArrayList<Tuple> tuples = re_1.getTuples();
			for (Tuple tuple : tuples) {
				
				
				assertTrue("", tuple.getField(0).hashCode() == 200);
			
			}
		}
		
		
		
		@Test
		
		/*
		 * We 'd like to test if the rename method would work 
		 * We renane 'a1' to the 'newname '
		 * 
		 */
		public void testRename()throws Exception {
			Relation ar = new Relation(ahf.getAllTuples(), atd);
			
			ArrayList<Integer> f = new ArrayList<Integer>();
			ArrayList<String> n = new ArrayList<String>();
			
			f.add(0);
			n.add("newname");
			
			
		
			
			ar = ar.rename(f, n);
			
			assertTrue(ar.getTuples().size() == 8);
			assertTrue(ar.getDesc().getFieldName(0).equals("newname"));
			assertTrue(ar.getDesc().getFieldName(1).equals("a2"));
			assertTrue(ar.getDesc().getSize() == 8);
		
		}
	
		

}
