import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;



import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
public class WriterThread implements Runnable {
	String fileName;
	long threadHashCode;
	HashMap<Long, int[]> threadStatus;
	int start,end;
	private synchronized void errorinfo(Connection con,Exception e,int rownum)
	{
		PreparedStatement ps=null;
		try {
			ps=con.prepareStatement("insert into errortable values(?,?)");
			ps.setInt(1, rownum);
			ps.setString(2, e.getMessage());
			if(ps.executeUpdate()==1)
			{
				System.out.println("Error Msg Inserted");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	private synchronized void serialize()
	{
		FileOutputStream fileOut;
		try {
		
			fileOut = new FileOutputStream("ser_files/write_record.ser");
			 ObjectOutputStream out = new ObjectOutputStream(fileOut);
			 out.writeObject(threadStatus);
			 out.close();
			 fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 	
		}
	}
	public void run()
	{  
		Connection con=null;
		CSVReader csvReader=null;
 		try {
 			Reader reader = Files.newBufferedReader(Paths.get(fileName));		 		    
 			csvReader =new CSVReaderBuilder(reader).withSkipLines(start).build();  		 		     
 			con=C3P0DataSource.getInstance().getConnection();
 			PreparedStatement ps=null;
 			for(long i=start;i<=end;i++)
 			{  
 				String[] data=csvReader.readNext();
			    try 
			    {
			    	ps=con.prepareStatement("insert into pg values(?,?,?,?,?)");
				    ps.setInt(1,Integer.parseInt(data[0]));
				    ps.setString(2,(data[1]));
				    ps.setString(3,(data[2]));
				    ps.setString(4,(data[3]));
				    ps.setInt(5,Integer.parseInt(data[4]));
				    int r=ps.executeUpdate();
				    if(r==1) {
				    	int[] recordStatus=threadStatus.get(threadHashCode);
						recordStatus[2]+=1;
				    	System.out.println("/****************************************Processed  " +i+ "th Record********************************************************************************/");
			    	}
			    }
			    catch (SQLException e) {
					// TODO Auto-generated catch block
			    	errorinfo(con,e,(int) i);
				}
			    finally {
			    	try {
					   if(ps!=null)
					   {
						   ps.close();
						   serialize();
					   }
					} 
			    	catch (SQLException e) {
						// TODO Auto-generated catch block
				   		e.printStackTrace();
				   		errorinfo(con,e,(int) i);
					}
			    	
   				} 
			}
	   	}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			errorinfo(con,e,start);
   		}
 		finally 
 		{
 			try {
 				if(con!=null)
 				{
 					con.close();
 				}
 				if(csvReader!=null)
 				{
 					csvReader.close();
 				}
 			} catch (SQLException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 				errorinfo(con,e,start);
 			} catch (IOException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			}
	   }			
	}  
	public void setIndex(int s,int e,HashMap<Long, int[]> threadStatus)
	{
		this.threadStatus=threadStatus;
		start=s;
		end=e;
	}
	WriterThread(String file)
	{
		threadHashCode=this.hashCode();
		fileName=file;
	}
}
