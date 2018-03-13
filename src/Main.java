import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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
	private static HashMap<Long, int[]> deSerialize() {
		HashMap<Long, int[]> prevThreadStatus=null;
		File file=new File("ser_files/write_record.ser");
		if(file.exists())
		{
			try {
				FileInputStream fileIn = new FileInputStream("ser_files/write_record.ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				prevThreadStatus = (HashMap<Long, int[]>) in.readObject();
				in.close();
				fileIn.close();
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		return prevThreadStatus;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HashMap<Long, int[]> prevThreadStatus=deSerialize();
		String fileName="doc/pg5.csv";
		Scanner sc=new Scanner(System.in);
		MutableInt numRecords = new MutableInt();
		MutableInt numPage=new MutableInt();
		MutableInt remRecords=new MutableInt();
		int remainingRecord;
		HashMap<Long,int[]> threadStatus=new HashMap<Long,int[]>();
		ExecutorService execService=Executors.newFixedThreadPool(NO_OF_CORES);
		int start,end;
		Instant starttime = Instant.now();
		if(prevThreadStatus!=null)
		{
			for(Map.Entry<Long,int[]> entry:prevThreadStatus.entrySet()){    
		        int[] b=entry.getValue();  
		        if(b[2]<b[1])
		        {
		        	start=b[2]+1;
		        	end=b[1];
		        	WriterThread thread=new WriterThread(fileName);
					threadStatus.put((long)(thread.hashCode()),new int[] {start,end,start-1});
					thread.setIndex(start, end,threadStatus);
					execService.execute(thread);
		        }
		    }
		}
		else
		{
			System.out.print("Enter Number of Thread:");
			int numOfThread=sc.nextInt();
			File file=new File(fileName);
			pageCalculations(numRecords,numPage,remRecords,numOfThread,file);
			int numOfRecord=numRecords.intValue();
			int numPages=numPage.intValue();
			remainingRecord=remRecords.intValue();
			int i;
			for(i=0;i<numOfThread;i++)
			{
				start=i*numPages+1;
				end=start+numPages-1;
				WriterThread thread=new WriterThread(fileName);
				threadStatus.put((long)(thread.hashCode()),new int[] {start,end,start-1});
				thread.setIndex(start, end,threadStatus);
				execService.execute(thread);
			}
			while(remainingRecord!=0)
			{
				start=numOfRecord-remainingRecord+1;
				end=numOfRecord-remainingRecord+1;
				WriterThread thread=new WriterThread(fileName);
				threadStatus.put((long)(thread.hashCode()),new int[] {start,end,start});
				thread.setIndex(start, end,threadStatus);
				execService.execute(thread);
				remainingRecord--;
			}
		}	
		execService.shutdown();  
		while (!execService.isTerminated()) {   } 
		boolean status=true;
		for(Map.Entry<Long, int[]> entry:threadStatus.entrySet()){    
	        int[] b=entry.getValue();  
	        if(b[2]<b[1])
	        {
	        	status=false;
	        	break;
	        }
	    }
		if(status)
		{
			try {
				if(Files.deleteIfExists(Paths.get("ser_files/write_record.ser")))
				{
					System.out.println("File Deleted");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		sc.close();
		Instant endtime = Instant.now();
		System.out.println(Duration.between(starttime, endtime));
	}
}