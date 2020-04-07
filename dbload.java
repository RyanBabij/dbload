import java.io.*; // file operations
import java.util.Vector; // vectors obvs
import java.util.concurrent.TimeUnit; //code execution timing
import java.nio.ByteBuffer; // convert int to bytes

public class dbload {

    public static void main(String[] args) {
    	
    	int pageSize = 0;
    	String file = "";
    	
    	long totalRecords=0;
    	int currentPage=0;
    	int currentByte=0;

        System.out.println("CSV heap database loader");
        
        if (args.length!=2)
        {
        	System.out.println("Error: Program must provide input file and pagesize arguments.");
        }
        else
        {
           file = args[0];
           
           if (file.length()==0)
           {
        	   System.out.println("Error: Bad filename arg.");
        	   System.exit(1);
           }
           
           System.out.println("Reading file: "+file);
           try
           {
                pageSize = Integer.parseInt(args[1]);
            }
            catch (NumberFormatException nfe)
           {
                System.out.println("Error: Pagesize must be an integer.");
                System.exit(1);
            }

        }
        
        // check that pageSize is valid.
        
        if ( pageSize <= 0 )
        {
        	System.out.println("Error: Pagesize must be greater than 0");
        	System.exit(1);
        }
        
        // From here we should have valid args. Begin loading the file into the heap.
        
        String outputFile = "heap."+Integer.toString(pageSize);
        System.out.println("Output file will be: "+outputFile);
        
        try
        (InputStream fileIn = new FileInputStream(file);
         OutputStream fileOut = new FileOutputStream(outputFile);)
        {
        	
        	System.out.println("Reading in file: "+file);
        	
        	// start timer
        	long startTime = System.nanoTime();
        	
        	// comma or newlines mean next column (nColumns is hardcoded)
        	// it seems lines are terminating with both \n and \r. I think
        	// we can just check one of them.

        	
        	// current field we are building to push to page.
        	Vector<Integer> vCurrentField = new Vector<>();
        	
        	boolean quotes=false; // commas in quotes are to be considered part of the string.
        	
            int byteRead;
            while (true)
            {
            	byteRead = fileIn.read(); // -1 means EOF

            	if ((quotes && byteRead == ',') || byteRead == 10 /* newline */ || byteRead == -1 ) // delimiter or end of file
            	{
            		// comma/newline found, which delimits the data. From here the data must be
            		// determined to be either an int or a string.
            		// CSVs sometimes escape commas using "," however the given data
            		// does not seem to have these errant commas.
            		
            		++totalRecords;
            		
            		//special case, zero-length vector. Just write a comma.
            		// check if 1 byte is free on the page
            		
            		// page files must always end in a comma. Everything beyond the last comma
            		// is null data
            		
            		if ( vCurrentField.size()==0)
            		{
            			fileOut.write(',');
            		}
            		else
            		{
                		// process the data here, including nulls.
                		// my binary file will use commas to delimit the data in the heap,
                		// so if it's a string, we can just copy it as-is.

                		// if the data is a number, it is 4 bytes, plus 1 byte for delimiter
                		// if the data is a string, it is string.length()+ 1 byte for the delimiter
                		
                		// Check if vector data is a string or ASCII number.
                		boolean isNumeric=true;
                		for (int i:vCurrentField)
                		{
                			if (i>=48 && i<=57)
                			{}
                			else
                			{
                				isNumeric=false;
                				break;
                			}
                		}
                		
                		if ( isNumeric )
                		{
                			// check if 5 bytes are free on the page
                			
                			//if we can't fit the data here, move to next page
                			// and fill rest of current page with null
                			if (currentByte+5 > pageSize)
                			{
                				while(currentByte<pageSize)
                				{
                					fileOut.write(0);
                					++currentByte;
                				}
                				currentByte=0;
                				++currentPage;
                			}
                			// write the data
                			
                			// lots of conversions here. Would probably be faster
                			// to just use DataStream
                			// push integer to page, or overflow to next page.
                			String strNumber="";
                    		for (int i:vCurrentField)
                    		{
                    			strNumber += (char)i;
                    		}
                    		ByteBuffer b = ByteBuffer.allocate(4);
                    		b.putInt(Integer.parseInt(strNumber));
                    		byte[] result = b.array();
                    		
                    		// write each of the 4 integer bytes
                    		fileOut.write(result[0]);
                    		fileOut.write(result[1]);
                    		fileOut.write(result[2]);
                    		fileOut.write(result[3]);
                  
                    		fileOut.write(',');
                    		currentByte+=5;

                		}
                		else
                		{
                			// check if vector length+1 bytes are free on the page
                			
                			//if we can't fit the data here, move to next page
                			// and fill rest of current page with null
                			if (currentByte+vCurrentField.size()+1 > pageSize)
                			{
                				while(currentByte<pageSize)
                				{
                					fileOut.write(0);
                					++currentByte;
                				}
                				currentByte=0;
                				++currentPage;
                			}
                			// write the data

                			// push string to page, or overflow to next page.
                    		for (int i:vCurrentField)
                    		{
                    			fileOut.write(i);
                    		}
                    		fileOut.write(',');
                    		currentByte+=vCurrentField.size()+1;
                		}
            		}
            		
            		vCurrentField.clear();
            		
            		if ( byteRead == -1 )
            		{
            			System.out.println("End of file reached.");
            			break;
            		}
            		
            	}
            	else if (byteRead == 13 ) // some editors insert this into documents. Ignore.
            	{
            	
            	}
            	else
            	{
            		if (byteRead==',') // ignore commas inside quotes
            		{
            			quotes = !quotes;
            		}
                	// push to vector until we find a delimiter
                	vCurrentField.add(byteRead);
            	}

            }

            ++totalRecords;
    		++currentPage;
            
            // stop timer
    		long endTime = System.nanoTime();

    		long totalNanoseconds = endTime - startTime;
    		long totalMilliseconds = totalNanoseconds/1000000;

    		System.out.println("File took "+totalMilliseconds+" milliseconds to import.");
    		System.out.println("Program imported: "+totalRecords+" records.");
    		System.out.println("Program created: "+currentPage+" pages.");
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        
    }

}