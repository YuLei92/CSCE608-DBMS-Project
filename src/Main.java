import java.util.Scanner;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;


public class Main {
    public static void main(String[] args){
        try {
            Scanner scan = new Scanner(System.in);


            Executor executor = new Executor();
            ProcessQuery process_query = new ProcessQuery();

            System.out.println("--------------TinySQL  Starts-----------------------");
            System.out.println("Enter 1 for commands, enter 2 for file, enter 3 for bottom test");
            String model = scan.nextLine();
            if (model.equalsIgnoreCase("1")) {
                System.out.println("Enter lines of commands");
                while (scan.hasNextLine()) {
                    executor.execute(scan.nextLine());
                }
            } else if (model.equalsIgnoreCase("2")) {
                File file = new File("./src/test.txt");
                Scanner scan2 = new Scanner(new FileInputStream(file));
                while (scan2.hasNextLine()) {
                    executor.execute(scan2.nextLine());
                }
            } else if(model.equalsIgnoreCase("3")){
                File file = new File("./src/test.txt");
                Scanner scan2 = new Scanner(new FileInputStream(file));
                System.out.println("Test the Process Query");
                System.out.println("Enter lines of commands");
                while (scan2.hasNextLine()) {
                    process_query.process(scan2.nextLine());
                }
            }
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
