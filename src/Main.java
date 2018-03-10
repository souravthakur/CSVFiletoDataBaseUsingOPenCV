import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.mutable.MutableInt;

public class Main {
	private static void pageCalculations(MutableInt numOfRecord,MutableInt numPages,MutableInt remainingRecord,int numOfThread,File file) {
		numOfRecord.setValue(countLineNumber(file));
		numPages.setValue((numOfRecord.intValue())/numOfThread);
		remainingRecord.setValue((numOfRecord.intValue())-numOfThread*(numPages.intValue()));	
	}
	private static int countLineNumber(File file)
	{
		int lines = 0;
		try {
			LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));
			lineNumberReader.skip(Long.MAX_VALUE);
	        lines = lineNumberReader.getLineNumber();
	      	lineNumberReader.close();
		}catch (IOException e) {
	        System.out.println("IOException Occurred" + e.getMessage());
	    }
		return lines-1;
	}
	private static final int NO_OF_CORES=2;
	@SuppressWarnings("unchecked")
	public static void deSerialize() {
		HashMap<Long, int[]> e = null;
		 try
		 {
		 FileInputStream fileIn = new FileInputStream("ser_files/write_record.ser");
		 ObjectInputStream in = new ObjectInputStream(fileIn);
		 e = (HashMap<Long, int[]>) in.readObject();
		 in.close();
		 fileIn.close();
		 }catch(IOException i)
		 {
		 i.printStackTrace();
		 return;
		 }catch(ClassNotFoundException c)
		 {
		 System.out.println("Employee class not found");
		 c.printStackTrace();
		 return;
		 }
		 for(Map.Entry<Long, int[]> entry:e.entrySet()){    
		        Long key=entry.getKey();  
		        int[] b=entry.getValue();  
		        System.out.println(key+" Details:");  
		        System.out.println(b[0]+" "+b[1]+" "+b[2]);   
		    }
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		deSerialize();
		System.out.print("Enter Number of Thread:");
		HashMap<Long,int[]> threadStatus=new HashMap<Long,int[]>();
		Scanner sc=new Scanner(System.in);
		int numOfThread=sc.nextInt();
		Instant starttime = Instant.now();
		String fileName="doc/pg5.csv";
		File file=new File(fileName);
		MutableInt numRecords = new MutableInt();
		MutableInt numPage=new MutableInt();
		MutableInt remRecords=new MutableInt();
		pageCalculations(numRecords,numPage,remRecords,numOfThread,file);
		int numOfRecord=numRecords.intValue();
		int numPages=numPage.intValue();
		int remainingRecord=remRecords.intValue();
		ExecutorService execService=Executors.newFixedThreadPool(NO_OF_CORES);
		int start,end;
		int i;
		for(i=0;i<numOfThread;i++)
		{
			start=i*numPages+1;
			end=start+numPages-1;
			WriterThread thread=new WriterThread(fileName);
			threadStatus.put((long) thread.hashCode(),new int[] {start,end,0});
			thread.setIndex(start, end,threadStatus);
			execService.execute(thread);
		}
		while(remainingRecord!=0)
		{
			start=numOfRecord-remainingRecord+1;
			end=numOfRecord-remainingRecord+1;
			WriterThread thread=new WriterThread(fileName);
			threadStatus.put((long) thread.hashCode(),new int[] {start,end,0});
			thread.setIndex(start, end,threadStatus);
			execService.execute(thread);
			remainingRecord--;
		}
		execService.shutdown();  
		while (!execService.isTerminated()) {   }  
		
		sc.close();
		Instant endtime = Instant.now();
		System.out.println(Duration.between(starttime, endtime));
	}
}