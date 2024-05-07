package org.santoryu.kafka.streams;

public class test {
    public static Integer gcd(Integer Big, Integer Small){
        System.out.println("Big " + Big);
        System.out.println("Small " + Small);
        return Big % Small == 0 ? Small : gcd(Small, Big % Small);
    }

    public static void main(String[] args) {
        Integer result = gcd(16, 8);
        System.out.println(result);
        Integer result2 = gcd(40, 24);
        System.out.println(result2);
    }
}
