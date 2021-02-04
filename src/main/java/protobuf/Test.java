package protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import utils.CommonUtils;

import java.io.*;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws Exception {
        common_SerializeAndDeserialize_mem();
        common_SerializeAndDeserialize_disk();
        proto_SerializeAndDeserialize();

        //---------------------------
        proto_ser_gps();
        proto_ser_person();
    }

    /////////////////////////////////////////////////
    public static void common_SerializeAndDeserialize_mem() throws IOException, ClassNotFoundException {
        Person person = new Person();
        person.setPid(1);
        person.setPname("小明");
        person.setScore(98.8);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(person);
        byte[] bytes = bos.toByteArray();
        System.out.println("bytes = " + Arrays.toString(bytes));
        System.out.println("序列化成功！！！");
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
        Person personNew = (Person) ois.readObject();
        System.out.println("反序列化成功！！！");
        System.out.println("personNew = " + personNew.toString());
    }


    //序列化
    public static void common_SerializeAndDeserialize_disk() throws Exception {
        Person person = new Person();
        person.setPid(1);
        person.setPname("小明");
        person.setScore(98.8);
        // ObjectOutputStream 对象输出流，将Person对象存储到E盘的Person.txt文件中，完成对Person对象的序列化操作
        ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(new File(CommonUtils.getResourcesPath() + "Person.txt")));
        oo.writeObject(person);
        System.out.println("Person对象序列化成功！");
        oo.close();


        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(CommonUtils.getResourcesPath() + "Person.txt")));
        Person persona = (Person) ois.readObject();
        System.out.println("Person对象反序列化成功！");
        System.out.println("person = " + persona.toString());
    }


    public static void proto_SerializeAndDeserialize() throws Exception {
        //serialize
        PersonModel.Person.Builder builder = PersonModel.Person.newBuilder();
        builder.setPid(2);
        builder.setPname("德玛");
        builder.setScore(88.9);
        PersonModel.Person person = builder.build();
        byte[] serialize = person.toByteArray();
        System.out.println("序列化成功！！！");
        System.out.println("bytes = " + Arrays.toString(serialize));

        //deserialize
        PersonModel.Person personb = PersonModel.Person.parseFrom(serialize);
        PersonModel.Person persona = personb;
        System.out.println("反序列化成功！！！");
        System.out.println("person = " + persona.getPid() + "*" + persona.getPname() + "*" + persona.getScore());
    }

    /////////////////////////////////////////////////


    public static void proto_ser_gps() {
        System.out.println("===== 构建一个GPS模型开始 =====");
        GPS.gps_data.Builder gps_builder = GPS.gps_data.newBuilder();
        gps_builder.setAltitude(1);
        gps_builder.setDataTime("2020-12-17 16:21:44");
        gps_builder.setGpsStatus(1);
        gps_builder.setLat(39.123);
        gps_builder.setLon(120.112);
        gps_builder.setDirection(30.2F);
        gps_builder.setId(100L);

        GPS.gps_data gps_data = gps_builder.build();
        System.out.println(gps_data.toString());
        System.out.println("===== 构建GPS模型结束 =====");

        System.out.println("===== gps Byte 开始=====");
        for (byte b : gps_data.toByteArray()) {
            System.out.print(b);
        }
        System.out.println("\n" + "bytes长度" + gps_data.toByteString().size());
        System.out.println("===== gps Byte 结束 =====");

        System.out.println("===== 使用gps 反序列化生成对象开始 =====");
        GPS.gps_data gd = null;
        try {
            gd = GPS.gps_data.parseFrom(gps_data.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        System.out.print(gd.toString());
        System.out.println("===== 使用gps 反序列化生成对象结束 =====");
    }

    /////////////////////////////////////////////////
    public static void proto_ser_person() {
        //1、 创建Builder
        Persons.Staff.Builder builder = Persons.Staff.newBuilder();
        //2、 设置Person的属性
        builder.setAge(20);
        builder.setName("java的架构师技术栈");
        //3、 创建Person
        Persons.Staff staff = builder.build();
        //4、序列化
        byte[] data = staff.toByteArray();
        //5、将data保存在本地或者是传到网络
        try {
            //一行代码实现反序列化，data可以是本地数据或者是网络数据
            Persons.Staff persona = Persons.Staff.parseFrom(data);
            System.out.println(persona.getAge());
            System.out.println(persona.getName());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

    }
    /////////////////////////////////////////////////


}
