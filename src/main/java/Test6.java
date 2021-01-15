import java.util.Map;

public class Test6 {
    public static void main(String[] args) {


        for (Map.Entry<String, String> stringStringEntry : System.getenv().entrySet()) {
            System.out.println(stringStringEntry.getKey() + "=" + stringStringEntry.getValue());
        }

        System.out.println("------------");


        System.setProperty("hahahh", "nihaohao ");

        for (Map.Entry<Object, Object> objectObjectEntry : System.getProperties().entrySet()) {
            System.out.println(objectObjectEntry.getKey() + "=" + objectObjectEntry.getValue());
        }


        System.out.println(System.nanoTime());
        System.out.println(System.currentTimeMillis());
        System.gc();




    }
}
