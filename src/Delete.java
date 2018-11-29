import java.util.ArrayList;

public class Delete {

    public OperationTree where_Clause;//where 后面跟的语句
    public String where_clause_string;
    public Delete(){
        where_Clause = new OperationTree();
        where_clause_string = null;
    }
    public Delete(String str){
        where_Clause = new OperationTree();
        where_clause_string = null;

        String[] strings = str.split(" ");

        int where_index = -1;

        for(int i=0;i<strings.length;i++){//找到有没有where
            if(strings[i].equalsIgnoreCase("where")){
                where_index = i;
            }
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = where_index + 1; i < strings.length; i++) {//
            stringBuilder.append(strings[i] + " ");
        }
        String where_string = stringBuilder.toString();
        OperationPriority op = new OperationPriority();
        where_Clause = op.get_operation_tree(where_string);
        where_clause_string = where_string;
    }
}

