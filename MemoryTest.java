import java.util.ArrayList;
import java.util.List;

public class MemoryTest {
    public static void main(String[] args) {
        // Trigger GC and print baseline memory usage
        Runtime runtime = Runtime.getRuntime();
        System.gc();
        long beforeUsedMem = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Before list creation: " + (beforeUsedMem / 1024 / 1024) + " MB");

        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            list.add("Record_" + i);
        }

        System.gc(); // Optional: request GC after list is populated
        long afterUsedMem = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("After list creation: " + (afterUsedMem / 1024 / 1024) + " MB");
        System.out.println("Memory used by list: " + ((afterUsedMem - beforeUsedMem) / 1024 / 1024) + " MB");
    }
}
