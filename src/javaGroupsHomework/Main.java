package javaGroupsHomework;

public class Main {
	
	public static void main(String[] args) throws Exception{

		System.out.println("Hello World");
		
		if ( args.length > 0)
		{
		   JavaGroupNode node = new JavaGroupNode(Integer.parseInt(args[0]));
		   node.connect();
		}
	}

}
