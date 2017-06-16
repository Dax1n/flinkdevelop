import java.util.ArrayList;
import java.util.List;

/**
 * Created by Daxin on 2017/4/18.
 */
public class Main {

    public static void main(String[] args) {

        String str1= "aaa";
        String str2=new String("aaa");

        System.out.println(str1==str2.intern());

        List<Integer> list1=new ArrayList();
        List<String> list2=new ArrayList();

        getString("");
        getString();


    }


    public static String getString(String ...str){

        for(int i =0;i<str.length;i++){
            System.out.println(" -> "+i);
        }

        return "";
    }
}
