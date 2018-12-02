import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.Hashtable;

import storageManager.*;
import java.util.*;
import java.util.*;

//use new_relation, temp_relation, result relation as the temp relation for operation

public class ProcessQuery {
    private String new_relation_name = "new_relation";
    private String r_name_1 = "relation_1";
    private String r_name_2 = "relation_2";
    private String r_result = "result_relation";

    private Parser parser;
    private ArrayList<String> attr_belong = new ArrayList<String>();
    private ArrayList<String> origin_name = new ArrayList<String>();

    public String query;
    public MainMemory mem;
    public Disk disk;
    public SchemaManager schema_manager;
    public ProcessQuery(){
        parser = new Parser();
        mem = new MainMemory();
        disk = new Disk();
        schema_manager =new SchemaManager(mem , disk);
        System.out.print("The memory contains " + mem.getMemorySize() + " blocks" + "\n");
        System.out.print(mem + "\n" + "\n");
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public void process(String query_){
        query = query_;
        System.out.print("Test Insert\n");
        parser.commandSplit(query);
        System.out.print("Keyword: " + parser.keyword+"\n");
        System.out.print("TableList: " + parser.table_List+"\n");

        if(schema_manager.relationExists("new_relation")){
            schema_manager.deleteRelation("new_relation");
        }

        if(schema_manager.relationExists("temp_relation")){
            schema_manager.deleteRelation("temp_relation");
        }

        if(schema_manager.relationExists("relation_1")){
            schema_manager.deleteRelation("relation_1");
        }

        if(schema_manager.relationExists("relation_1")){
            schema_manager.deleteRelation("relation_2");
        }

        if(schema_manager.relationExists("result_relation")){
            schema_manager.deleteRelation("result_relation");
        }

        switch (parser.keyword.get(0)){
            case "create" : op_create();
                            break;
            case "drop"   : op_drop();
                            break;
            case "insert" : op_insert();
                            break;
            case "delete" : op_delete();
                            break;
            case "select" : op_select();
                            break;

            default: System.out.println("Nothing to do.\n\n");
        }
    }

    private void natural_join(Relation r1, Relation r2, Relation result_r, String attr){
        FieldType attr_type = r1.getSchema().getFieldType(attr);
        String r1_temp_name = r_name_1;
        String r2_temp_name = r_name_2;
        //先依次对r1, r2进行sort，然后存回。
        Schema r1_schema = r1.getSchema();
        Schema r2_schema = r2.getSchema();
        Relation r1_new = schema_manager.createRelation(r_name_1, r1_schema);
        Relation r2_new = schema_manager.createRelation(r_name_2, r2_schema);
        int r1_block_num = r1.getNumOfBlocks();
        int r2_block_num = r2.getNumOfBlocks();
        int scan_times = (r1_block_num - 1) / 10 + 1; //计算需要进行多少次scan
        int op_block_num; //存储每次对多少个block进行操作
        for(int i = 0; i < scan_times; i++) {
            if ((i + 1) * 10 > r1_block_num) {
                op_block_num = r1_block_num - i * 10;
            } else {
                op_block_num = 10;
            }//计算每次block操作个数
            r1.getBlocks(i * 10, 0, op_block_num);//接下来对这个进行排序
            int prev_tuple_num = 0;
            Block curr_block, prev_block;
            Tuple curr_tuple, prev_tuple;
            prev_block = mem.getBlock(0);
            prev_tuple = prev_block.getTuple(0);
            for (int end_block_num = op_block_num - 1; end_block_num >= 0; end_block_num--) {
                for (int end_tuple_num = mem.getBlock(end_block_num).getNumTuples() - 1; end_tuple_num >= 0; end_tuple_num--) {
                    for (int curr_block_num = 0; curr_block_num <= end_block_num; curr_block_num++) {
                        curr_block = mem.getBlock(curr_block_num); //得到当前block
                        if(curr_block_num == 0){ //如果是此次循环的第0个block，初始化prev相关内容
                            prev_block = mem.getBlock(0);
                            prev_tuple = prev_block.getTuple(0);
                            prev_tuple_num = 0;
                        }

                        for (int curr_tuple_num = 0; curr_tuple_num <= end_tuple_num; curr_tuple_num++) {
                            curr_tuple = curr_block.getTuple(curr_tuple_num); //得到当前的tuple
                            if (curr_block_num == 0 && curr_tuple_num == 0) {
                                continue;   //如果是最初那个tuple，直接略过
                            }

                            int comp_r;
                            if(attr_type == FieldType.INT){
                                comp_r = prev_tuple.getField(attr).integer - curr_tuple.getField(attr).integer;
                            }else{
                                comp_r = prev_tuple.getField(attr).toString().compareTo(curr_tuple.getField(attr).toString());
                            }

                            if (comp_r >= 0) { //如果需要交换，交换各自block的对应tuple, 并将当前tuple设置为交换后的
                                Tuple temp_tuple = prev_tuple;
                                prev_block.setTuple(prev_tuple_num, curr_tuple);
                                curr_block.setTuple(curr_tuple_num, temp_tuple);
                                curr_tuple = temp_tuple;
                            }

                            prev_tuple = curr_tuple;
                            prev_block = curr_block;
                            prev_tuple_num = curr_tuple_num;
                            //更新prev三件套
                        }
                        prev_block = mem.getBlock(curr_block_num); //更新block在每次循环后，这里是否需要重新修改？
                    }
                }
            }//对r1冒泡排序
            r1_new.setBlocks(i * 10, 0, op_block_num);//存入r1_new
        }

        scan_times = (r2_block_num - 1) / 10 + 1; //计算需要进行多少次scan
        for(int i = 0; i < scan_times; i++) {
            if ((i + 1) * 10 > r2_block_num) {
                op_block_num = r2_block_num - i * 10;
            } else {
                op_block_num = 10;
            }//计算每次block操作个数
            r2.getBlocks(i * 10, 0, op_block_num);//接下来对这个进行排序
            int prev_tuple_num = 0;
            Block curr_block, prev_block;
            Tuple curr_tuple, prev_tuple;
            prev_block = mem.getBlock(0);
            prev_tuple = prev_block.getTuple(0);
            for (int end_block_num = op_block_num - 1; end_block_num >= 0; end_block_num--) {
                for (int end_tuple_num = mem.getBlock(end_block_num).getNumTuples() - 1; end_tuple_num >= 0; end_tuple_num--) {
                    for (int curr_block_num = 0; curr_block_num <= end_block_num; curr_block_num++) {
                        curr_block = mem.getBlock(curr_block_num); //得到当前block
                        if(curr_block_num == 0){ //如果是此次循环的第0个block，初始化prev相关内容
                            prev_block = mem.getBlock(0);
                            prev_tuple = prev_block.getTuple(0);
                            prev_tuple_num = 0;
                        }

                        for (int curr_tuple_num = 0; curr_tuple_num <= end_tuple_num; curr_tuple_num++) {
                            curr_tuple = curr_block.getTuple(curr_tuple_num); //得到当前的tuple
                            if (curr_block_num == 0 && curr_tuple_num == 0) {
                                continue;   //如果是最初那个tuple，直接略过
                            }

                            int comp_r;
                            if(attr_type == FieldType.INT){
                                comp_r = prev_tuple.getField(attr).integer - curr_tuple.getField(attr).integer;
                            }else{
                                comp_r = prev_tuple.getField(attr).toString().compareTo(curr_tuple.getField(attr).toString());
                            }

                            if (comp_r >= 0) { //如果需要交换，交换各自block的对应tuple, 并将当前tuple设置为交换后的
                                Tuple temp_tuple = prev_tuple;
                                prev_block.setTuple(prev_tuple_num, curr_tuple);
                                curr_block.setTuple(curr_tuple_num, temp_tuple);
                                curr_tuple = temp_tuple;
                            }

                            prev_tuple = curr_tuple;
                            prev_block = curr_block;
                            prev_tuple_num = curr_tuple_num;
                            //更新prev三件套
                        }
                        prev_block = mem.getBlock(curr_block_num); //更新block在每次循环后，这里是否需要重新修改？
                    }
                }
            }//对r2冒泡排序
            r2_new.setBlocks(i * 10, 0, op_block_num);//存入r2_new
        }

        int num_r1 = r1.getNumOfBlocks();
        int num_r2 = r2.getNumOfBlocks();
        int num_r1_new = r1_new.getNumOfBlocks();
        int num_r2_new = r2_new.getNumOfBlocks();

        System.out.print("Now the result relation r1 contains: " + "\n");
        System.out.print(r1+ "\n" + "\n");

        System.out.print("Now the result relation r1_new contains: " + "\n");
        System.out.print(r1_new+ "\n" + "\n");

        System.out.print("Now the result relation r2 contains: " + "\n");
        System.out.print(r2+ "\n" + "\n");

        System.out.print("Now the result relation r2_new contains: " + "\n");
        System.out.print(r2_new+ "\n" + "\n");
        
    }


    private  Relation find_largest_relation(ArrayList<Relation> relations){
        int block_least_num = -1;
        Relation result = null;
        for(Relation temp_relation : relations){
            if(temp_relation.getNumOfBlocks() > block_least_num){
                result = temp_relation;
            }
        }
        return result;
    }

    private void op_one_pass(Relation r_1, Relation r_2, Relation result_Relation, OperationTree op_tree, String ops){
        if(r_2 == null){
            //单独对r_1进行操作

        }

        int block_num_1 = r_1.getNumOfBlocks();
        int block_num_2 = r_2.getNumOfBlocks();
        boolean r_1_isLarge = false;
        Relation R, S; //R是较小的块， S是较大的块
        if(block_num_1 >= block_num_2){
            r_1_isLarge = true;
            R = r_2;
            S = r_1;
        }else{
            R = r_1;
            S = r_2;
        }

        //用第9个块依次存较大的个数，用剩余的块依次存较小的内容(如果本身小于8个就直接全部放入了）。使用one pass
        int scan_times_R= (R.getNumOfBlocks() - 1) / 8 + 1;
        int op_block_num;
        for(int i = 0; i < scan_times_R; i++){
            if((i+1) * 8 > R.getNumOfBlocks()){
                op_block_num = R.getNumOfBlocks() - i * 8;
            }else {
                op_block_num = 8;
            }
            R.getBlocks(i * 8, 0, op_block_num); //将R中至多8个块存入。
            for(int j = 0; j < S.getNumOfBlocks(); j++){
                S.getBlock(j, 8);
                Block block_S = mem.getBlock(8);
                for(int k = 0; k < op_block_num; k++){
                    Block block_R = mem.getBlock(k);
                    if(r_1_isLarge){
                        meta_two_block_op(block_S, block_R, result_Relation, op_tree, ops);
                    }else{
                        meta_two_block_op(block_R, block_S, result_Relation, op_tree, ops);
                    }
                }
            }
        }
    }

    private void meta_two_block_op(Block b_1, Block b_2, Relation relation, OperationTree op_tree, String ops){
        //进行操作
        switch (ops) {
            case "cross_join":
                meta_cross_join(b_1, b_2, relation, op_tree);
                break;
        }
    }

    private void meta_cross_join(Block b_1, Block b_2, Relation result_Relation, OperationTree op_tree){
        for(Tuple tuple_1 : b_1.getTuples()){
            for(Tuple tuple_2 : b_2.getTuples()){
                Tuple new_tuple = result_Relation.createTuple();
                for(int tuple_offset = 0; tuple_offset < tuple_1.getNumOfFields(); tuple_offset++){
                    if(tuple_1.getSchema().getFieldType(tuple_offset) == FieldType.INT){
                        int value = tuple_1.getField(tuple_offset).integer;
                        new_tuple.setField(tuple_offset, value);
                    }else{
                        String value = tuple_1.getField(tuple_offset).str;
                        new_tuple.setField(tuple_offset, value);
                    }
                }

                for(int tuple_offset = 0; tuple_offset < tuple_2.getNumOfFields(); tuple_offset++){
                    if(tuple_2.getSchema().getFieldType(tuple_offset) == FieldType.INT){
                        int value = tuple_2.getField(tuple_offset).integer;
                        new_tuple.setField(tuple_offset + tuple_1.getNumOfFields(), value);
                    }else{
                        String value = tuple_2.getField(tuple_offset).str;
                        new_tuple.setField(tuple_offset + tuple_1.getNumOfFields(), value);
                    }
                }
                if(op_tree == null) { //如果没有where条件
                    appendTupleToRelation(result_Relation, mem, 9, new_tuple); //用第10个内存块进行操作
                }else{
                    if(where_test(op_tree, new_tuple, result_Relation)){
                        appendTupleToRelation(result_Relation, mem, 9, new_tuple);
                    }
                }
            }
        }
    }

    private Relation crossjoin(ArrayList<Relation> relations, OperationTree op_tree){
        //use the 10th block in the memory to do the insert.
        if(relations.size() == 0 || relations.size() == 1){
            return null;
        }
        Schema schema_1, schema_2;
        String name_new, name_old;
        Relation relation_1, relation_2;
        if(relations.size() <= 2){
            origin_name.clear();
        }
        boolean isBottom = false;
        if(relations.size() == 2 ){
            name_new = r_name_1;
            name_old = null;
            relation_1 = relations.get(0);
            relation_2 = relations.get(1);
            schema_1 = relation_1.getSchema();
            schema_2 = relation_2.getSchema();
            isBottom = true;
        }else{
            // r1存递归结果, r2存更大的relation
            relation_2 = find_largest_relation(relations);
            schema_2 = relation_2.getSchema();
            relations.remove(relation_2);
            relation_1 = crossjoin(relations, op_tree);
            schema_1 = relation_1.getSchema();
            if(relation_1.getRelationName().equalsIgnoreCase(r_name_2)){
                name_new = r_name_1;
                name_old = r_name_2;
            }else{
                name_new = r_name_2;
                name_old = r_name_1;
            }
        }

        ArrayList<String> field_names = new ArrayList<String>();
        ArrayList<FieldType> field_types = new ArrayList<FieldType>();

        for(String new_name : schema_1.getFieldNames()){
            field_names.add(new_name);
            field_types.add(schema_1.getFieldType(new_name));

            if(isBottom) {
                origin_name.add(new_name); //加入原本的名字
            }
        }

        // r_2是一个新表
        for(String new_name : schema_2.getFieldNames()){
            if(origin_name.contains(new_name)){ //如果和加前缀前的名字有重复的
                if(isBottom){
                    int old_pos = origin_name.indexOf(new_name);
                    field_names.set(old_pos, relation_1.getRelationName() + "." + new_name);
                }
                field_names.add(relation_2.getRelationName() + "." + new_name);
            }else {
                field_names.add(new_name);
            }
            field_types.add(schema_2.getFieldType(new_name));
            origin_name.add(relation_2.getRelationName());
        }



        Schema schema_new = new Schema(field_names, field_types);
        Relation result_relation = schema_manager.createRelation(name_new, schema_new);
        op_one_pass(relation_1, relation_2, result_relation, op_tree, "cross_join");

        if(!isBottom){
            schema_manager.deleteRelation(name_old);
        }

        return  result_relation;
    }



    //进度：仅仅实现单个table的select
    private Relation op_select(){
        Relation result_relation = null;
        Schema result_schema, target_schema;
        Select select_array = parser.select;
        int op_block_num;
        Block temp_block;

        if(select_array.table_List.size() <= 1){
            System.out.print("\nTry to do single select: \n");
            Relation target_relation = schema_manager.getRelation(select_array.table_List.get(0));
            target_schema = target_relation.getSchema();
            int block_num = target_relation.getNumOfBlocks();
            if(block_num == 0){
                return result_relation;
            }
            int scan_times= (block_num - 1) / 9 + 1;
            //判断如果是简单块
            if(select_array.select_List.size() == 1 && select_array.select_List.get(0).equals("*")){
                result_schema = schema_manager.getSchema(select_array.table_List.get(0));//得到schema
                result_relation = schema_manager.createRelation("new_relation",result_schema); //建立一个新的表

                for(int i = 0; i < scan_times; i ++){
                    if((i+1) * 9 > block_num){
                        op_block_num = block_num - i * 9;
                        schema_manager.getRelation(select_array.table_List.get(0)).getBlocks(i * 9, 0, op_block_num);
                    }else{
                        op_block_num = 9;
                        schema_manager.getRelation(select_array.table_List.get(0)).getBlocks(i * 9, 0, op_block_num);
                    } //从disk中读入选择relation的9个块，并设定为目标块的。

                    if(select_array.where_clause_string == null){
                        result_relation.setBlocks( i * 9, 0, op_block_num);
                    }else{
                        for(int j = 0; j < op_block_num; j++){
                            temp_block = mem.getBlock(j); //对每个块进行操作
                            int tuple_count = temp_block.getNumTuples();
                            for(int k = 0; k < tuple_count; k++){
                                Tuple temp_tuple = temp_block.getTuple(k);
                                if(where_test(select_array.where_Clause ,temp_tuple ,target_relation)){
                                    appendTupleToRelation(result_relation, mem, 9,temp_tuple);
                                }
                            }
                        }
                    }
                }

                //如果select只有*， 直接操作后返回result_relation
                System.out.print("Now the result relation contains: " + "\n");
                System.out.print(result_relation+ "\n" + "\n");
//                return result_relation;

            }else {  //如果不是单纯的 *
                ArrayList<String> field_names = new ArrayList<String>();
                ArrayList<FieldType> field_types = new ArrayList<FieldType>();
                String temp_name;
                for(String field_name : select_array.select_List){
                    temp_name = field_name.substring(field_name.lastIndexOf('.') + 1);
                    field_names.add(temp_name);
                    field_types.add(target_schema.getFieldType(temp_name));
                }
                result_schema = new Schema(field_names, field_types);
                result_relation = schema_manager.createRelation("new_relation", result_schema); //初始化result schema

//                Schema temp_schema = schema_manager.getSchema(select_array.table_List.get(0));
//                Relation temp_relation = schema_manager.createRelation("temp_relation",temp_schema); // 初始化temp schema
                Tuple temp_tuple, new_tuple;
                int block_tuple_num;

                for(int i = 0; i < scan_times; i ++){
                    if((i+1) * 9 > block_num){
                        op_block_num = block_num - i * 9;
                    }else {
                        op_block_num = 9;
                    }
                    schema_manager.getRelation(select_array.table_List.get(0)).getBlocks(i * 9, 0, op_block_num);
//                    temp_relation.setBlocks(i*9, 0, op_block_num);
                    //从disk中读入选择relation的至多9个块，并设定为目标块的。
                    for(int j = 0; j < op_block_num; j++){
                        //依次对每个块进行操作
                        temp_block = mem.getBlock(j);
                        block_tuple_num = temp_block.getNumTuples();
                        for(int k = 0; k < block_tuple_num; k++){
                            //依次对每个tuple进行操作
                            temp_tuple = temp_block.getTuple(k);
                            new_tuple = result_relation.createTuple();
                            for(String field_name : field_names){
                                if(target_schema.getFieldType(field_name) == FieldType.STR20){
                                    new_tuple.setField(field_name, temp_tuple.getField(field_name).str);
                                }else{
                                    new_tuple.setField(field_name, temp_tuple.getField(field_name).integer);
                                }
                            } //把每个元素存进新的new_tuple
                            appendTupleToRelation(result_relation, mem, 9,new_tuple);
                        }
                    }

                }
                System.out.print("The table currently have " + result_relation.getNumOfTuples() + " tuples" + "\n");
                System.out.print("Now the result relation contains: " + "\n");
                System.out.print(result_relation+ "\n" + "\n");

//                return  result_relation;
            }
        }else{

            natural_join(schema_manager.getRelation(select_array.table_List.get(0)), schema_manager.getRelation(select_array.table_List.get(1)),
                    null, "exam");
            ArrayList<Relation> relations = new ArrayList<Relation>();


            for(String temp_r : select_array.table_List){
                relations.add(schema_manager.getRelation(temp_r));
            } //得到新的list包含所有的关系

            Relation temp_relation;

            if(select_array.where_clause_string == null) {
                //如果没有where
                temp_relation = crossjoin(relations,null);
            }else{
                temp_relation = crossjoin(relations, select_array.where_Clause);
            }

            if(select_array.select_List.size() == 1 && select_array.select_List.get(0).equals("*")){
                //如果多个表但是只有"*"
                result_relation = temp_relation;
            }else{
                ArrayList<String> field_names = new ArrayList<String>();
                ArrayList<FieldType> field_types = new ArrayList<FieldType>();
                for(String field_name : select_array.select_List){
                    field_names.add(field_name);
                    field_types.add(temp_relation.getSchema().getFieldType(field_name));
                }
                result_schema = new Schema(field_names, field_types);
                result_relation = schema_manager.createRelation(r_result, result_schema); //初始化result schema

                Tuple temp_tuple, new_tuple;
                int block_num = temp_relation.getNumOfBlocks();
                int scan_times= (block_num - 1) / 9 + 1;
                int block_tuple_num;

                for(int i = 0; i < scan_times; i ++){
                    if((i+1) * 9 > block_num){
                        op_block_num = block_num - i * 9;
                    }else {
                        op_block_num = 9;
                    }
                    schema_manager.getRelation(temp_relation.getRelationName()).getBlocks(i * 9, 0, op_block_num);
 //                   temp_relation.setBlocks(i*9, 0, op_block_num);
                    //从disk中读入选择relation的至多9个块，并设定为目标块的。
                    for(int j = 0; j < op_block_num; j++){
                        //依次对每个块进行操作
                        temp_block = mem.getBlock(j); //依次对每个block进行操作
                        block_tuple_num = temp_block.getNumTuples();
                        for(int k = 0; k < block_tuple_num; k++){
                            //依次对每个tuple进行操作
                            temp_tuple = temp_block.getTuple(k);
                            new_tuple = result_relation.createTuple();
                            for(String field_name : field_names){
                                if(result_schema.getFieldType(field_name) == FieldType.STR20){
                                    new_tuple.setField(field_name, temp_tuple.getField(field_name).str);
                                }else{
                                    new_tuple.setField(field_name, temp_tuple.getField(field_name).integer);
                                }
                            } //把每个元素存进新的new_tuple
                            appendTupleToRelation(result_relation, mem, 9,new_tuple);
                        }
                    }

                }
            }
        }

        System.out.print("The table currently have " + result_relation.getNumOfTuples() + " tuples" + "\n");
        System.out.print("Now the result relation contains: " + "\n");
        System.out.print(result_relation+ "\n" + "\n");

        return result_relation;
    }

    private void op_delete(){
        System.out.print("\nStart to delete.\n");
        String relation_name = parser.table_List.get(0);
        if(parser.delete.where_clause_string == null) { //实现不包含where的情况
            schema_manager.deleteRelation(relation_name);

            System.out.print("\nTable " + relation_name + " has been deleted.\n");

            //The following print is just the test.
            System.out.print("\nCurrent schemas and relations: " + "\n");
            System.out.print(schema_manager + "\n");
            System.out.print("From the schema manager, the table " + relation_name + " exists: "
                    + (schema_manager.relationExists(relation_name) ? "TRUE" : "FALSE") + "\n");
        }else{
            Relation target_relation = schema_manager.getRelation(relation_name);
            OperationTree op_tree = parser.delete.where_Clause;
            
            Tuple temp_tuple, last_tuple;
            Block temp_block, last_block;
            //依次取出delete的block进行处理

            for(int i = 0; i < target_relation.getNumOfBlocks(); i++){
                int block_num = target_relation.getNumOfBlocks();
                target_relation.getBlock(i, 0); //找到要处理的block放入0中;
                temp_block = mem.getBlock(0);

                target_relation.getBlock(target_relation.getNumOfBlocks() - 1, 9);//找到最后一个block放入最后
                last_block = mem.getBlock(9);
                int last_num = target_relation.getNumOfBlocks() - 1;
                for(int j = 0; j < temp_block.getNumTuples(); ){
                    int tuple_num = temp_block.getNumTuples();
                    temp_tuple = temp_block.getTuple(j);
                    if(where_test(op_tree, temp_tuple, target_relation)){ //如果满足条件
                        if( i == (target_relation.getNumOfBlocks() - 1)){
                            temp_tuple = temp_block.getTuple(temp_block.getNumTuples() - 1);
                            temp_block.setTuple(j, temp_tuple);
                            temp_block.invalidateTuple(temp_block.getNumTuples() - 1); //将最后一个tuple设置为不可用
                            //如果是最后一个block填坑情况
                        }else{
                            //如果不是最后一个block的填坑情况。
                            if(last_block.getNumTuples() == 0){
                                target_relation.deleteBlocks(last_num); //删除最后一个block.
                                target_relation.getBlock(target_relation.getNumOfBlocks() - 1, 9);//找到最后一个block放入最后
                                last_block = mem.getBlock(9);
                            } //更新block.
                            temp_tuple = last_block.getTuple(last_block.getNumTuples() - 1); //赋值最后一个temp_tuple
                            temp_block.setTuple(j, temp_tuple);
                            last_block.invalidateTuple(last_block.getNumTuples() - 1); //将最后一个block的位置消除
                        }
                    }else{
                        j++;
                    }
                }
                target_relation.setBlock(i, 0);
                if( i != (target_relation.getNumOfBlocks() - 1)){
                    //如果不是对最后一个block进行删除操作，传回最后一个block，如果空，直接删除
                    if(last_block.getNumTuples() != 0) {
                        target_relation.setBlock(target_relation.getNumOfBlocks() - 1, 9);
                    }else{
                        target_relation.deleteBlocks(target_relation.getNumOfBlocks() - 1); //删除最后一个block.
                    }
                }

//                target_relation.setBlocks(i * 10 + index, 0, op_nums); //将mem中10个blocks传入。
            }
            System.out.print("The table currently have " + target_relation.getNumOfTuples() + " tuples" + "\n");
            System.out.print("Now the result relation contains: " + "\n");
            System.out.print(target_relation+ "\n" + "\n");
        }
    }


    private String where_dfs(OperationTree op_tree, Tuple tuple, Relation r1){
        if(op_tree.op == null){
            return null;
        }
        if(op_tree.left_tree == null && op_tree.right_tree == null){
            Schema schema = tuple.getSchema();
            String temp_name;
            temp_name = op_tree.op;
            if(op_tree.op.contains(".")){
                int pos = op_tree.op.lastIndexOf('.') + 1;
                int length = op_tree.op.length();
                if(op_tree.op.substring(0, op_tree.op.lastIndexOf('.') + 1).equalsIgnoreCase(r1.getRelationName())){
                    temp_name =  op_tree.op.substring(op_tree.op.lastIndexOf('.') + 1);
                }
            }

            if(schema.fieldNameExists(temp_name)){
                return tuple.getField(temp_name).toString();
            }else{
                return temp_name;
            }
        }
        int temp;
        switch (op_tree.op){
            case "=" :
                if(where_dfs(op_tree.left_tree, tuple, r1).equalsIgnoreCase(where_dfs(op_tree.right_tree, tuple, r1))){
                    return "true";
                }else{
                    return "false";
                }
            case "&&" :
                if(where_dfs(op_tree.left_tree, tuple, r1).equalsIgnoreCase("true") &&
                        where_dfs(op_tree.right_tree, tuple, r1).equalsIgnoreCase("true")){
                    return "true";
                }else{
                    return "false";
                }
            case "||" :
                if(where_dfs(op_tree.left_tree, tuple, r1).equalsIgnoreCase("true") ||
                        where_dfs(op_tree.right_tree, tuple, r1).equalsIgnoreCase("true")){
                    return "true";
                }else{
                    return "false";
                }
            case ">" :
                if(Integer.parseInt(where_dfs(op_tree.left_tree, tuple, r1)) >
                        Integer.parseInt(where_dfs(op_tree.right_tree, tuple, r1))){
                    return "true";
                }else{
                    return "false";
                }
            case "<" :
                if(Integer.parseInt(where_dfs(op_tree.left_tree, tuple, r1)) <
                    Integer.parseInt(where_dfs(op_tree.right_tree, tuple, r1))){
                return "true";
                }else{
                    return "false";
                }

            case "+" :
                temp = Integer.parseInt(where_dfs(op_tree.left_tree, tuple, r1)) +
                        Integer.parseInt(where_dfs(op_tree.right_tree, tuple, r1));
                return Integer.toString(temp);

            case "-" :
                temp = Integer.parseInt(where_dfs(op_tree.left_tree, tuple, r1)) -
                        Integer.parseInt(where_dfs(op_tree.right_tree, tuple, r1));
                return Integer.toString(temp);

            case "*" :
                temp = Integer.parseInt(where_dfs(op_tree.left_tree, tuple, r1)) *
                        Integer.parseInt(where_dfs(op_tree.right_tree, tuple, r1));
                return Integer.toString(temp);

            case "/" :
                temp = Integer.parseInt(where_dfs(op_tree.left_tree, tuple, r1)) /
                        Integer.parseInt(where_dfs(op_tree.right_tree, tuple, r1));
                return Integer.toString(temp);

            default: System.out.println("Nothing to do.\n\n");
        }
        return null;
    }

    //需要进行修改，现在只是简单判断结果
    private boolean where_test(OperationTree op_tree, Tuple tuple, Relation r1) {
        String result = where_dfs(op_tree, tuple, r1);
        boolean result_temp = result.equalsIgnoreCase("true");
        return result.equalsIgnoreCase("true");
    }

    //realize insert without select
    private void op_insert(){
        System.out.print("\nStart to insert.\n");
        String relation_name = parser.table_List.get(0);
        Relation target_relation = schema_manager.getRelation(relation_name);
        Schema target_schema = target_relation.getSchema();
        Tuple tuple = target_relation.createTuple();
        if(parser.select.table_List.size() == 0){
            String temp_name;
            for(int traverse_count = 0; traverse_count < parser.value_List.size(); traverse_count++) {
                temp_name = parser.argument_List.get(traverse_count).name;
                System.out.print("The name is : "+ temp_name + ".\n");
                if (target_schema.getFieldType(temp_name) == FieldType.STR20) {
                    System.out.print("Insert str20.\n");
                    tuple.setField(temp_name, parser.value_List.get(traverse_count));
                } else if (target_schema.getFieldType(temp_name) == FieldType.INT) {
                    System.out.print("Insert int.\n");
                    if(parser.value_List.get(traverse_count).equalsIgnoreCase("null")){
                        tuple.setField(temp_name, -1);
                    }else{
                        tuple.setField(temp_name, Integer.parseInt(parser.value_List.get(traverse_count)));
                        System.out.print("The int is : "+ Integer.parseInt(parser.value_List.get(traverse_count)) + ".\n");
                    }
                }
            }
            appendTupleToRelation(target_relation, mem, 9,tuple);
        }else if(parser.select.table_List.size() == 1){ //先解决如果只有一个single table的情况
            Relation select_relation = op_select();
            int block_nums = select_relation.getNumOfBlocks();
            int index = target_relation.getNumOfBlocks();
            int scan_times= (block_nums - 1) / 10 + 1;
            int op_nums;

            for(int i = 0; i < scan_times; i++){
                if((i+1) * 10 > block_nums){
                    op_nums = block_nums - i * 10;
                }else {
                    op_nums = 10;
                }
                select_relation.getBlocks(i * 10, 0, op_nums); //用10个block进行
                target_relation.setBlocks(i * 10 + index, 0, op_nums); //将mem中10个blocks传入。
            }
        }

        System.out.print("Now the memory contains: " + "\n");
        System.out.print(mem + "\n");
        System.out.print("Now the relation contains: " + "\n");
        System.out.print(target_relation + "\n" + "\n");
    }
    private  void op_drop(){
        System.out.print("\nStart to drop.\n");
        String relation_name = parser.table_List.get(0);
        schema_manager.deleteRelation(relation_name);
        System.out.print("\nTable " + relation_name + " has been deleted.\n");

        //The following print is just the test.
        System.out.print("\nCurrent schemas and relations: " + "\n");
        System.out.print(schema_manager + "\n");
        System.out.print("From the schema manager, the table " + relation_name + " exists: "
                + (schema_manager.relationExists(relation_name)?"TRUE":"FALSE") + "\n");

    }

    private void op_create(){
        System.out.print("\nStart to create.\n");
        ArrayList<String> field_names = new ArrayList<String>();
        ArrayList<FieldType> field_types = new ArrayList<FieldType>();
        String relation_name =  parser.table_List.get(0);
//        System.out.print("Argument size: " + parser.argument_List.size());

        for(Argument temp_argu : parser.argument_List) {
            field_names.add(temp_argu.name);
//            System.out.print("\nType: " + temp_argu.type);
            if (temp_argu.type.equals("STR20")) {
                field_types.add(FieldType.STR20);
            } else if (temp_argu.type.equals("INT")) {
                field_types.add(FieldType.INT);
            }
        }

        Schema schema=new Schema(field_names,field_types);
        Relation relation_reference=schema_manager.createRelation(relation_name,schema);

        System.out.print("The schema has " + schema.getNumOfFields() + " fields" + "\n");
        System.out.print("The schema allows " + schema.getTuplesPerBlock() + " tuples per block" + "\n");

        System.out.print("\nThe created table has name " + relation_reference.getRelationName() + "\n");
        System.out.print("The table has schema:" + "\n");
        System.out.print(relation_reference.getSchema() + "\n");
        System.out.print("The table currently have " + relation_reference.getNumOfBlocks() + " blocks" + "\n");
        System.out.print("The table currently have " + relation_reference.getNumOfTuples() + " tuples" + "\n" + "\n");

        //The following print is just the test.
        System.out.print("\nCurrent schemas and relations: " + "\n");
        System.out.print(schema_manager + "\n");
        System.out.print("\nFrom the schema manager, the table " + relation_name + " exists: "
                + (schema_manager.relationExists(relation_name)?"TRUE":"FALSE") + "\n");
        System.out.print("\nFrom the schema manager, the table " + relation_name + " has schema:" + "\n");
        System.out.print(schema_manager.getSchema(relation_name) + "\n");
        System.out.print("\nFrom the schema manager, the table " + relation_name + " has schema:" + "\n");
        System.out.print(schema_manager.getRelation(relation_name).getSchema() + "\n");

    }

    private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
    Block block_reference;
        if (relation_reference.getNumOfBlocks()==0) {
            System.out.print("The relation is empty" + "\n");
 //           System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
            block_reference=mem.getBlock(memory_block_index);
            block_reference.clear(); //clear the block
            block_reference.appendTuple(tuple); // append the tuple
//            System.out.print("Write to the first block of the relation" + "\n");
            relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
        } else {
//            System.out.print("Read the last block of the relation into memory block 5:" + "\n");
            relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
            block_reference=mem.getBlock(memory_block_index);

            if (block_reference.isFull()) {
//                System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
                block_reference.clear(); //clear the block
                block_reference.appendTuple(tuple); // append the tuple
//                System.out.print("Write to a new block at the end of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
            } else {
 //               System.out.print("(The block is not full: Append it directly)" + "\n");
                block_reference.appendTuple(tuple); // append the tuple
  //              System.out.print("Write to the last block of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
            }
        }
    }
}
