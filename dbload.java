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
        
        
    }

}