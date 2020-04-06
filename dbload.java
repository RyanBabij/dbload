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
        	
        	// create the pagefile to build
        	//int remainingBytes = pageSize;
        	//int aByte[pageSize];
        	int aByte[]=new int[pageSize];
        	int indexByte=0; // current location on page array
        	
        	// current field we are building to push to page.
        	Vector<Integer> vCurrentField = new Vector<>();
        	
            int byteRead;
            while ((byteRead = fileIn.read()) != -1)
            {
            	if (byteRead == ',')
            	{
            		System.out.println("STOP... COMMATIME");
            	}
            	else if (byteRead == '\n' || byteRead == '\r')
            	{
            		System.out.println("Ignoring newline... What newline?");
            	}
            	else
            	{
                	// push to vector until we find a comma. Then check if it will fit onto page.
                	vCurrentField.add(byteRead);
                	System.out.println("DATA");
            	}

            }
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        
    }

}