import java.util.*;
import java.util.ArrayList;

public class Executor {

    public Parser parser;

    public Executor(){
        parser = new Parser();
    }
    public void execute(String str){
        System.out.println(str);
        parser.commandSplit(str);//分割命令
        System.out.println("keyword");
        for(int i = 0; i < parser.keyword.size(); i++) {
            System.out.println(parser.keyword.get(i));
        }
        System.out.println("tableList" );
        for(int i = 0; i < parser.table_List.size(); i++) {
            System.out.print(parser.table_List.get(i)+ "  ");
        }
        System.out.println("\n"+"valueList");
        for(int i = 0; i < parser.value_List.size(); i++) {
            System.out.print(parser.value_List.get(i)+ "  ");
        }
        System.out.println("\n"+"argumentList" );
        for(int i = 0; i < parser.argument_List.size(); i++) {
                System.out.print(parser.argument_List.get(i).name + " ");
            System.out.print(parser.argument_List.get(i).type + " ");
        }
        System.out.println("\n"+"delete" );

        System.out.println("\n"+"delete.where_Clause: " );
        System.out.println(parser.delete.where_clause_string );

        System.out.println("\n"+"delete.where op: " );
        System.out.println(parser.delete.where_Clause);

        System.out.println("\n"+"select" );

        if(parser.select.distinct){
            System.out.println("\n"+"Distinct" );
        }else{
            System.out.println("\n"+"No Distinct" );
        }
        System.out.println("\n"+"select.Column_Name_List" );
        for(int i = 0; i < parser.select.select_List.size(); i++) {
            System.out.print(parser.select.select_List.get(i)+ " ");
        }
        System.out.println("\n"+"select.From_Table_List" );
        for(int i = 0; i < parser.select.table_List.size(); i++) {
            System.out.print(parser.select.table_List.get(i)+ " ");
        }
        System.out.println("\n"+"select.where_Clause: " );
        System.out.println(parser.select.where_clause_string );

        System.out.println("\n"+"select.Order_Clause: " );
        System.out.println(parser.select.order_Clause );



    }

}
