import java.io.*; // file operations
import java.util.Vector; // vectors obvs

public class dbload {

    public static void main(String[] args) {
    	
    	int pageSize = 0;
    	String file = "";

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
        	
        	// ignore newlines. commas mean next column (nColumns is hardcoded)
        	

        	int aByte[]=new int[pageSize];
        	int indexByte=0; // current location on page array
        	
        	// current field we are building to push to page.
        	Vector<Integer> vCurrentField = new Vector<>();
        	
            int byteRead;
            while ((byteRead = fileIn.read()) != -1)
            {
            	if (byteRead == ',')
            	{
            		// comma found, which delimits the data. From here the data must be
            		// determined to be either an int or a string.
            		// CSVs sometimes escape commas using "," however the given data
            		// does not seem to have these errant commas.
            		
            		System.out.println("STOP... COMMATIME. Checking loaded field.");
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
            			System.out.println("Field is an integer");
            			// push integer to page, or overflow to next page.
            		}
            		else
            		{
            			System.out.println("Field is a string");
            			// push string to page, or overflow to next page.
            		}
            		vCurrentField.clear();
            		
            	}
            	else if (byteRead == '\n' || byteRead == '\r')
            	{
            		System.out.println("Ignoring newline... What newline?");
            	}
            	else
            	{
                	// push to vector until we find a comma
                	vCurrentField.add(byteRead);
                	System.out.println("DATA");
            	}

            }
            System.out.println("Process final field here");
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        
    }

}