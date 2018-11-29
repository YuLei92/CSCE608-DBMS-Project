import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.Hashtable;

import storageManager.*;
import java.util.*;
import java.util.*;

public class ProcessQuery {
    private String new_relation_name = "new_relation";
    private Parser parser;
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

    //进度：仅仅实现单个table的select
    private Relation op_select(){
        Relation result_relation = null;
        Schema result_schema;
        Select select_array = parser.select;
        if(schema_manager.relationExists("new_relation")){
            schema_manager.deleteRelation("new_relation");
        }
        if(select_array.table_List.size() == 1 && select_array.where_clause_string == null){
            result_schema = schema_manager.getSchema(select_array.table_List.get(0));//得到schema
            result_relation = schema_manager.createRelation("new_relation",result_schema); //建立一个新的表
            System.out.print("\nTry to do single select: \n");
            Relation target_relation = schema_manager.getRelation(select_array.table_List.get(0));
            int block_num = target_relation.getNumOfBlocks();
            if(block_num == 0){
                return result_relation;
            }
            int scan_times= (block_num - 1) / 9 + 1;
            for(int i = 0; i < scan_times; i ++){
                if((i+1) * 9 > block_num){
                    schema_manager.getRelation(select_array.table_List.get(0)).getBlocks(i * 9, 0, block_num - i * 9);
                    result_relation.setBlocks( i * 9, 0, block_num - i * 9);
                }else{
                    schema_manager.getRelation(select_array.table_List.get(0)).getBlocks(i * 9, 0, 9);
                    result_relation.setBlocks(i*9, 0, 9);
                } //从disk中读入选择relation的9个块，并设定为目标块的。
            }
            //判断如果是简单块
            if(select_array.select_List.size() == 1 && select_array.select_List.get(0).equals("*")){
                System.out.print("Now the result relation contains: " + "\n");
                System.out.print(result_relation + "\n" + "\n");
                System.out.print("\nJust a simple select, try to return.\n");
                return result_relation;
            }
        }
        return result_relation;
    }

    private void op_delete(){
        System.out.print("\nStart to delete.\n");
        if(parser.delete.where_clause_string == null) { //实现不包含where的情况
            String relation_name = parser.table_List.get(0);
            schema_manager.deleteRelation(relation_name);

            System.out.print("\nTable " + relation_name + " has been deleted.\n");

            //The following print is just the test.
            System.out.print("\nCurrent schemas and relations: " + "\n");
            System.out.print(schema_manager + "\n");
            System.out.print("From the schema manager, the table " + relation_name + " exists: "
                    + (schema_manager.relationExists(relation_name) ? "TRUE" : "FALSE") + "\n");
        }
    }

    //realize insert without select
    //Insert需要完成的： 1.集成select语句
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
                        tuple.setField(temp_name, -999999999);
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
            for(int i = 0; i < block_nums; i++){
                select_relation.getBlock(i , 9); //用第9个block进行
                target_relation.setBlock(i + index, 9); //将mem中第9个传入。
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
            System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
            block_reference=mem.getBlock(memory_block_index);
            block_reference.clear(); //clear the block
            block_reference.appendTuple(tuple); // append the tuple
            System.out.print("Write to the first block of the relation" + "\n");
            relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
        } else {
            System.out.print("Read the last block of the relation into memory block 5:" + "\n");
            relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
            block_reference=mem.getBlock(memory_block_index);

            if (block_reference.isFull()) {
                System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
                block_reference.clear(); //clear the block
                block_reference.appendTuple(tuple); // append the tuple
                System.out.print("Write to a new block at the end of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
            } else {
                System.out.print("(The block is not full: Append it directly)" + "\n");
                block_reference.appendTuple(tuple); // append the tuple
                System.out.print("Write to the last block of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
            }
        }
    }
}
