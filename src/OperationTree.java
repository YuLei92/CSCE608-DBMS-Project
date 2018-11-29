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

}
