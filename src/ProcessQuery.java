import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.Hashtable;

import storageManager.*;
import java.util.*;
import java.util.*;

public class ProcessQuery {
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
    private void op_select(){

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
        Relation target_reation = schema_manager.getRelation(relation_name);
        Schema target_schema = target_reation.getSchema();
        Tuple tuple = target_reation.createTuple();
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
                    System.out.print("The int is : "+ Integer.parseInt(parser.value_List.get(traverse_count)) + ".\n");
                    tuple.setField(temp_name, Integer.parseInt(parser.value_List.get(traverse_count)));
                }
            }
        }
        appendTupleToRelation(target_reation, mem, 2,tuple);

        System.out.print("Now the memory contains: " + "\n");
        System.out.print(mem + "\n");
        System.out.print("Now the relation contains: " + "\n");
        System.out.print(target_reation + "\n" + "\n");
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
