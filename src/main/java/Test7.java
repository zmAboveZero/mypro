import java.util.Random;

public class Test7 {
    public static void main(String[] args) {

        normalDistribution(100, 1000);

    }

    //标准正态随机分布
    public static double standardNormalDistribution() {
        Random random = new Random();
        return random.nextGaussian();
    }

    //普通正态随机分布
    //参数 u 均值
    //参数 v 方差
    public static void normalDistribution(float u, float v) {
        Random random = new Random();
        Integer source = 0;
        Integer target = 0;

        for (int i = 0; i < 100; i++) {
            source=Integer.valueOf((int) Math.ceil(Math.sqrt(v) * random.nextGaussian() + u));
            target=Integer.valueOf((int) Math.ceil(Math.sqrt(v) * random.nextGaussian() + u));

            System.out.println(source+","+target);
        }




    }

}
