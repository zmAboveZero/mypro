package protobuf;

import com.google.protobuf.InvalidProtocolBufferException;

public class Test {
    public static void main(String[] args) {
        gps();
        person();
    }

    public static void gps() {
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

    public static void person() {
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
}
