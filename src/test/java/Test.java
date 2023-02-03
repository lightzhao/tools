import java.time.ZoneOffset;

public class Test {

    public static void main(String[] args) {
        ZoneOffset zoneOffset = ZoneOffset.of("Z");
        System.out.println(zoneOffset.toString());
    }
}
