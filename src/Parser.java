import java.util.ArrayList;

public class Parser {//Commandsplit函数把所有commznc分开
    ArrayList<String> keyword;
    ArrayList<String> value_List;
    ArrayList<String> table_List;
    ArrayList<Argument> argument_List;
    Select select;
    Delete delete;

    public Parser() {
        keyword = new ArrayList<>();
        value_List = new ArrayList<>();
        table_List = new ArrayList<>();
        select = new Select();
        delete= new Delete();
        argument_List = new ArrayList<>();
    }

    private void reset(){
        keyword = new ArrayList<>();
        value_List = new ArrayList<>();
        table_List = new ArrayList<>();
        select = new Select();
        delete= new Delete();
        argument_List = new ArrayList<>();

    }
    public void commandSplit(String str){

        this.reset();
        str = str.replace(",","");
        str = str.replace("\"","");
        String[] command = str.split(" ");

        //create 的情况
        if(command[0].equalsIgnoreCase("create") && command[1].equalsIgnoreCase("table")){
            keyword.add("create");
            table_List.add(command[2]);
            command[3]=command[3].replace("(","");
            for (int bracket_index = 3; bracket_index < command.length; bracket_index++) {
                if (command[bracket_index].indexOf(")") != -1) {//找到有）的元素
                    command[bracket_index] = command[bracket_index].replace(")", "");
                    for (int i = 3; i <= bracket_index - 1; i = i + 2) {
                        Argument arg = new Argument(command[i], command[i + 1]);//两个两个传
                        argument_List.add(arg);
                    }
                }
            }
        }

        //drop的情况
        if(command[0].equalsIgnoreCase("drop") && command[1].equalsIgnoreCase("table")){
            keyword.add("drop");
            table_List.add(command[2]);
        }

        //insert的情况
        if(command[0].equalsIgnoreCase("insert") && command[1].equalsIgnoreCase("into")){
            keyword.add("insert");
            table_List.add(command[2]);
            int value_index = -1;
            int select_index = -1;
            for(int i = 3; i < command.length; i++){
                if(command[i].equalsIgnoreCase("values")){//判断有没有value
                    value_index = i;
                }
                if(command[i].equalsIgnoreCase("select")){//判断有没有value
                    select_index = i;
                }
            }
            if(value_index >0){//insert中有value的情况
                for(int i = 3; i <value_index; i++){
                    command[i] = command[i].replace("(","");
                    command[i] = command[i].replace(")","");
                    Argument arg = new Argument(command[i],null);//两个两个传
                    argument_List.add(arg);
                }
                for(int i = value_index+1; i <command.length; i++){
                    command[i] = command[i].replace("(","");
                    command[i] = command[i].replace(")","");
                    value_List.add(command[i]);
                }

            }

            if(select_index >0){
                for(int i = 3; i <select_index; i++){
                    command[i] = command[i].replace("(","");
                    command[i] = command[i].replace(")","");
                    Argument arg = new Argument(command[i],null);//两个两个传
                    argument_List.add(arg);
                }
                StringBuilder stringBuilder = new StringBuilder();
                for(int i = select_index+1; i < command.length; i++){
                    stringBuilder.append(command[i]+" ");
                }
                String select_clause = stringBuilder.toString();
                select = new Select(select_clause);
            }
        }

        //delete的情况
        if(command[0].equalsIgnoreCase("delete") && command[1].equalsIgnoreCase("from")){
            keyword.add("delete");
            table_List.add(command[2]);
            int where_index = -1;
            for (int i = 0; i< command.length;i++) {
                if (command[i].equalsIgnoreCase("where")) {//delete里有where
                    where_index = i;
                }
            }
            if(where_index>-1){
                StringBuilder stringBuilder = new StringBuilder();

            for(int i= where_index; i < command.length; i++) {
                stringBuilder.append(command[i] + " ");
            }
            String delete_where_clause = stringBuilder.toString();
            delete =new Delete(delete_where_clause);
            }

        }

        if (command[0].equalsIgnoreCase("select")){
            keyword.add("select");
            StringBuilder stringBuilder = new StringBuilder();
            for(int i = 1; i < command.length; i++){
                stringBuilder.append(command[i]+" ");
            }
            String select_clause = stringBuilder.toString();
            select = new Select(select_clause);

        }


    }
}
