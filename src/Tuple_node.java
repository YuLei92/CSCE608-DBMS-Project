import storageManager.Tuple;

public class Tuple_node {
    public Tuple tuple;
    public int sublist_index; //第几个sublist
    public int block_index; //sublist中第几个block
    public int tuple_index; //Block中的第几个tuple
    public int mem_index;   //mem中在什么位置

    public Tuple_node(Tuple tuple, int sublist_index, int block_index, int tuple_index, int mem_index){
        this.tuple = tuple;
        this.sublist_index = sublist_index;
        this.block_index = block_index;
        this.tuple_index = tuple_index;
        this.mem_index = mem_index;
    }
}
