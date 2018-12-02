import storageManager.*;
import java.util.ArrayList;
import java.util.List;



public class heapHelper {
    List<String> compare_attr;
    public int max_size;
    public int size;
    Tuple_node[] heap;

    public heapHelper(int max_size, List<String> compare_attr){
        this.max_size = max_size;
        this.compare_attr = compare_attr;
        this.size = 0;
        heap = new Tuple_node[max_size];
    }

    public int parent(int index){
        if(index == 0){
            return -1;
        }else{
            return (index - 1) / 2;
        }
    }

    public int left_child(int index){
        return (index + 1) * 2 - 1;
    }

    public int right_child(int index){
        return (index + 1) * 2;
    }

    public boolean isLeaf(int index){
        if(index >= size / 2 && index < size){
            return true;
        }
        return false;
    }


    //压出最小的节点
    public Tuple_node pop(){
        if(heap.length == 0 || size == 0){
            System.out.println("Heap has no nodes.");
            return null;
        }
        swap(0, size - 1);
        size --;
        if(size == 0){
            return heap[0];
        }
        int curr = 0;
        int index_s; //存储更小节点的位置
        while(!isLeaf(curr)){
            index_s = right_child(curr);
            if(index_s<size) {
                if (compare_two_node(heap[index_s].tuple, heap[index_s - 1].tuple) > 0) {
                    index_s = index_s - 1;
                    if (compare_two_node(heap[curr].tuple, heap[index_s].tuple) > 0) {
                        swap(curr, index_s);
                        curr = index_s;
                    } else {
                        break;
                    }
                } else{
                    if (compare_two_node(heap[curr].tuple, heap[index_s].tuple) > 0) {
                        swap(curr, index_s);
                        curr = index_s;
                    } else {
                        break;
                    }
                }
            }else{
                index_s = index_s -1;
                if (compare_two_node(heap[curr].tuple, heap[index_s].tuple) > 0) {
                    swap(curr, index_s);
                    curr = index_s;
                } else {
                    break;
                }
            }
        }
        return heap[size];
    }

    public void insert(Tuple_node tuple_node){ //实现对新节点的插入操作
        size++;
        heap[size - 1] = tuple_node;
        int curr = size - 1; //插入新节点的位置
        while(parent(curr) >= 0 && compare_two_node(heap[curr].tuple, heap[parent(curr)].tuple) < 0){
            swap(curr, parent(curr)); //插入并交换
            curr = parent(curr);
        }
    }



    public void swap(int index_1, int index_2){
        Tuple_node temp = heap[index_2];
        heap[index_2] = heap[index_1];
        heap[index_1] = temp;
    }


    //比较两个节点
    public  int compare_two_node(Tuple curr, Tuple parent){
        Field field_curr =  curr.getField(compare_attr.get(0));
        Field field_parent = parent.getField(compare_attr.get(0));
        if(field_curr.type == FieldType.INT){
            return field_curr.integer - field_parent.integer;
        }else{
            return field_curr.str.compareTo(field_parent.str);
        }
    }

}
