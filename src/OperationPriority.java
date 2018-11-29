import java.util.*;;


public class OperationPriority {
    public static OperationTree get_operation_tree(String str) {
         String[] strings = str.split(" ");
         Stack<String> operator = new Stack<>();
         Stack<OperationTree> obejct = new Stack<>();
         for (int i = 0; i < strings.length; i++) {
             if (strings[i].equalsIgnoreCase("and")) {
                 strings[i] = "&&";
             }
             if (strings[i].equalsIgnoreCase("or")) {
                    strings[i] = "||";
             }
             if (strings[i].equalsIgnoreCase("not")) {
                    strings[i] = "!";
             }
         }

         for (int i = 0; i < strings.length; i++) {
             switch (strings[i]) {
                 case "!":
                 case "*":
                 case "/":
                 case "+":
                 case "-":
                 case "=":
                 case ">":
                 case "<":
                 case "&&":
                 case "||":
                     while (operator.isEmpty() == false && precedence(strings[i]) < precedence(operator.peek())) {
                            OperationTree right_tree = obejct.pop();
                            OperationTree left_tree = obejct.pop();
                            String op = operator.pop();
                            obejct.push(new OperationTree(left_tree, right_tree, op));
                     }
                     break;
                 case "(":
                 case "[":
                     operator.push(strings[i]);
                     break;
                 case ")":
                 case "]":
                     while (operator.isEmpty() == false && operator.peek().equalsIgnoreCase("(") == false && operator.peek().equalsIgnoreCase("[") == false) {
                         OperationTree right_tree = obejct.pop();
                         OperationTree left_tree = obejct.pop();
                         String op = operator.pop();
                         obejct.push(new OperationTree(left_tree, right_tree, op));
                     }
                     operator.pop();
                     break;
                 default:
                     obejct.push(new OperationTree(strings[i]));
                }
            }
            while (operator.isEmpty() == false) {
                    OperationTree right_tree = obejct.pop();
                    OperationTree left_tree = obejct.pop();
                    String op = operator.pop();
                    obejct.push(new OperationTree(left_tree, right_tree, op));
            }
            return obejct.pop();

        }


    public static int precedence(String str){
        switch(str){
            case "!":return 5;
            case "*":
            case "/":return 4;
            case "+":
            case "-":return 3;
            case "=":
            case ">":
            case "<":return 2;
            case "&&":return 1;
            case "||":return 0;
            default: return -1;
        }
    }
}
