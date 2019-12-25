package javaGroupsHomework;

public class Main {

    public static void main(String[] args) throws Exception {

        System.out.println("Hello World");

        if (args.length > 0) {
            int id = -1;

            try {
                id = Integer.parseInt(args[0]);
            } catch (NumberFormatException nfe) {
                System.out.println("Incorrect id format. Only numeric!");
                System.out.println(args[0]);
                return;
            }

            JavaGroupNode node = new JavaGroupNode(id);
            node.mainFunction();
            node.disconnect();
        }
    }

}
