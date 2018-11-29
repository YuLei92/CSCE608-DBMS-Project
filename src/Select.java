import java.util.ArrayList;

public class Select {



    public ArrayList<String> table_List;//table-list
    public OperationTree where_Clause;//where 后面跟的语句
    public String where_clause_string;//where clause 的string类型 仅用于验证
    public String order_Clause;//order后的语句
    public ArrayList<String> select_List; //select-list
    public boolean distinct;

    public Select(){
        where_Clause = new OperationTree();
        order_Clause = null;
        select_List = new ArrayList<>();
        table_List = new ArrayList<>();
        where_clause_string = null;
        distinct = false;
    }

    public Select(String str){
        where_Clause = new OperationTree();
        order_Clause = null;
        select_List = new ArrayList<>();
        table_List = new ArrayList<>();
        where_clause_string = null;
        distinct = false;

        String[] strings = str.split(" ");
        int where_index = -1;
        int order_index = -1;
        int from_index = -1;
        for(int i=0;i<strings.length;i++){//找到有没有where
            if(strings[i].equalsIgnoreCase("where")){
                where_index = i;
            }
            if(strings[i].equalsIgnoreCase("order")){
                order_index = i;
            }
            if(strings[i].equalsIgnoreCase("from")){
                from_index = i;
            }
        }

        if (where_index > -1){//有where没有order
            if(order_index == -1) {

                for(int i=from_index +1; i < where_index; i++){
                    table_List.add(strings[i]);
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
            if(order_index > -1) {//有where 有order

                for(int i=from_index +1; i < where_index; i++){
                    table_List.add(strings[i]);
                }

                StringBuilder stringBuilder = new StringBuilder();
                for (int i = where_index + 1; i < order_index; i++) {//
                    stringBuilder.append(strings[i] + " ");
                }
                String where_string = stringBuilder.toString();
                OperationPriority op = new OperationPriority();
                where_Clause = op.get_operation_tree(where_string);
                where_clause_string = where_string;

                StringBuilder stringBuilder1 = new StringBuilder();
                for (int i = order_index + 2; i < strings.length; i++) {//
                    stringBuilder1.append(strings[i] + " ");
                }
                order_Clause = stringBuilder1.toString();
            }
        }
        if(where_index == -1){//没有where
            if (order_index == -1){//没有order
                for(int i=from_index +1; i < strings.length; i++){
                    table_List.add(strings[i]);
                }
            }
            if (order_index > -1){// 有order

                for(int i=from_index +1; i < order_index; i++){
                    table_List.add(strings[i]);
                }

                StringBuilder stringBuilder1 = new StringBuilder();
                for (int i = order_index + 2; i < strings.length; i++) {//
                    stringBuilder1.append(strings[i] + " ");
                }
                order_Clause = stringBuilder1.toString();
            }
        }
        if (strings[0].equalsIgnoreCase("distinct")){
            distinct = true;
            for (int i = 1; i < from_index; i++){
                select_List.add(strings[i]);
            }
        }
        else{
            for (int i = 0; i < from_index; i++){
                select_List.add(strings[i]);
            }
        }
    }
}
