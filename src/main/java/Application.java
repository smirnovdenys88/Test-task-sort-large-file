import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class Application {
    private static Logger logger = Logger.getLogger(Application.class.getName());
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1; // 1 MB
    private static final int FILE_SEGMENT_SIZE = 1024; // 1 MB
    private static final int FILE_SIZE = 1024 * 16; // 10 MB
    private static Random random = new Random();
    private static int j = 0;
    private static int i = 0;
    private static String path = "/home/user/IdeaProjects/Learning/sort-large-file/src/main/resources";
    private static String pathOutFile = "/home/user/IdeaProjects/Learning/sort-large-file/src/main/resources/out.txt";
    private static List<File> files = new ArrayList<>();

    public static void main(String[] args) throws IOException, InterruptedException {
//        createLargeFile();
        cutFile();
        sortFile();
        mergeFile();
    }

    static void createLargeFile() throws IOException {
        logger.info("Start process createLargeFile finish");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(path + "/data.txt")));
        String str = "";
        while (str.length() < FILE_SIZE) {// 120 MB

            bufferedWriter.write(str);
            bufferedWriter.flush();
            if (str.length() == FILE_SIZE) {
                str += random.nextInt(Integer.MAX_VALUE);
            } else {
                str += random.nextInt(Integer.MAX_VALUE) + "\n";
            }
        }
        bufferedWriter.close();
        logger.info("Finish process createLargeFile");
    }

    static void cutFile() throws IOException {
        logger.info("Start process cutFile");
        FileChannel source = new FileInputStream(new File(path + "/data.txt")).getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
        File file = new File(path + "/temp-" + i + ".txt");
        files.add(file);

        FileChannel destination = new FileOutputStream(file).getChannel();
        while ((source.read(buf)) != -1) {
            buf.flip();
            destination.write(buf);
            if (j == FILE_SEGMENT_SIZE) {
                i++;
                destination.close();

                file = new File(path + "/temp-" + i + ".txt");
                files.add(file);

                destination = new FileOutputStream(file).getChannel();
                j = 0;
            } else {
                j++;
            }
            buf.clear();
        }
        source.close();
        logger.info("Finish process cutFile");
    }

    static void sortFile() throws IOException {
        logger.info("Start process sort by files");
        List<Integer> integers = new ArrayList<>();

        //Sort ever file
        for (j = 0; j <= i; j++) {
            BufferedReader br = new BufferedReader(new FileReader(files.get(j)));
            try {
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.isEmpty()) integers.add(Integer.valueOf(line));
                }
            } catch (IOException e) {
                System.err.format("IOException: %s%n", e);
            } finally {
                br.close();
            }

            Collections.sort(integers);
            BufferedWriter writer = new BufferedWriter(new FileWriter(files.get(j)));
            try {
                for (Integer itt : integers) {
                    writer.write(itt + "\n");
                    writer.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                writer.close();
            }
            integers.clear();
        }
        logger.info("Finish process sort by files");
    }

    private static void mergeFile() throws IOException {
        logger.info("Start process merge file");
        File out = new File(pathOutFile);

        List<Integer> integers = new ArrayList<>();
        List<BufferedReader> brs = new ArrayList<>();
        for (j = 0; j <= i; j++) {
            brs.add(new BufferedReader(new FileReader(files.get(j))));
        }

        boolean flag = true;
        FileWriter fr = new FileWriter(out, true);

        while (flag) {
            String line;
            for (j = 0; j <= i; j++) {
                try {
                    while ((line = brs.get(j).readLine()) != null) {
                        fr.write(line);
                        fr.write("\n");
                        fr.flush();
                        break;
                    }
                    if (j == 0) {
                        flag = line != null;
                    }
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            }
            integers.clear();
        }

        for (BufferedReader reader : brs) {
            reader.close();
        }

        for (File file : files) {
            Files.deleteIfExists(file.toPath());
        }

        fr.close();
        logger.info("Finish process merge file");
    }
}
