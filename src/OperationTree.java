import java.util.ArrayList;
public class OperationTree {

    OperationTree left_tree;
    OperationTree right_tree;
    String op;

    public OperationTree() {
        left_tree = null;
        right_tree = null;
        op = null;
    }

    public OperationTree(OperationTree left, OperationTree right, String op) {
        left_tree = left;
        right_tree = right;
        this.op = op;
    }

    public OperationTree(String op) {
        left_tree = null;
        right_tree = null;
        this.op = op;
    }
    public ArrayList<OperationTree> operationTreeList(){

        if(right_tree ==null){
            return null;
        }
        //这棵树的左数
        if((Character.isDigit(this.left_tree.op.charAt(0)) || Character.isLetter(this.left_tree.op.charAt(0))) && (Character.isDigit(this.right_tree.op.charAt(0)) || Character.isLetter(this.right_tree.op.charAt(0)))){
            ArrayList<OperationTree> res = new ArrayList<>();
            res.add(this);
            return res;
        }

        ArrayList<OperationTree> left_ex = this.left_tree.operationTreeList();
        ArrayList<OperationTree> right_ex = this.right_tree.operationTreeList();

        if (left_ex!=null){
            if(right_ex!=null){
                left_ex.addAll(right_ex);
            }
            return left_ex;
        }else{
            if(right_ex!=null){
                return right_ex;
            }
            return null;
        }
    }




}

