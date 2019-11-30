import javafx.beans.binding.StringBinding;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Application {
    private static Logger logger = Logger.getLogger(Application.class.getName());
    private static final int DEFAULT_BUFFER_SIZE = 1024; // 1 MB
    private static final int FILE_SEGMENT_SIZE = 66000; // 102,4 MB
    private static final long FILE_SIZE = 147000; // 1 GB
    private static Random random = new Random();
    private static int j = 0;
    private static int i = 0;
    private static String path = "/home/ann/IdeaProjects/Test-task-sort-large-file/src/main/resources";
    private static String pathOutFile = path + "/out.txt";
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
        StringBuffer stringBuilder = new StringBuffer();
        while (stringBuilder.length() <= FILE_SIZE) {
            stringBuilder.append(random.nextInt(Integer.MAX_VALUE));
            stringBuilder.append("\n");
            bufferedWriter.write(stringBuilder.toString());
            bufferedWriter.flush();
        }
        bufferedWriter.close();
        logger.info("Finish process createLargeFile");
    }

    static void cutFile() throws IOException {
        logger.info("Start process cutFile");

        FileChannel source = new FileInputStream(path + "/data.txt").getChannel();
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
        FileReader reader;
        BufferedReader br;

        FileWriter fileWriter;
        BufferedWriter writer;
        String line;
        for (j = 0; j <= i; j++) {
            reader = new FileReader(files.get(j));
            br = new BufferedReader(reader);
            try {
                while ((line = br.readLine()) != null) {
                    if (!line.isEmpty()) integers.add(Integer.valueOf(line));
                }
            } catch (IOException e) {
                System.err.format("IOException: %s%n", e);
            } finally {
                reader.close();
                br.close();
            }

            Collections.sort(integers);
            fileWriter = new FileWriter(files.get(j));
            writer = new BufferedWriter(fileWriter);
            try {
                for (Integer itt : integers) {
                    writer.write(itt + "\n");
                    writer.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                fileWriter.close();
                writer.close();
            }
            integers.clear();
        }
        logger.info("Finish process sort by files");
    }

    private static void mergeFile() throws IOException {
        logger.info("Start process merge file");
        File out = new File(pathOutFile);

        Map<Integer,Integer> indexValueTree;
        List<Integer> integers = new ArrayList<>();
        List<BufferedReader> brs = new ArrayList<>();
        Map<Integer,Integer> indexValue = new HashMap<>();

        for (j = 0; j <= i; j++) {
            brs.add(new BufferedReader(new FileReader(files.get(j))));
            indexValue.put(j, -1);
        }

        boolean flag = true;
        FileWriter fr = new FileWriter(out, true);

        String value;
        int stopRead = 0;
        int x = 0;
        boolean increment = false;

        while (flag) {
            for (j = 0; j <= i; j++)  {
                try {

                    if(stopRead == i){
                        flag = false;
                        break;
                    }
                    if(indexValue.get(j) > -1) continue;

                    value = brs.get(j).readLine();
                    if(value == null){
                        stopRead++;
                        continue;
                    }
                    indexValue.put(j, Integer.valueOf(value));
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            }

            indexValueTree = indexValue.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .limit(13)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            if(indexValueTree.isEmpty()) break;
            final Integer finalX = x;
            if(!indexValueTree.values().stream().filter(s -> s.equals(finalX)).findFirst().isPresent()) x = indexValueTree.values().stream().findFirst().get();

            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<Integer,Integer> intTree: indexValueTree.entrySet()){
                if(x != intTree.getValue()) continue;
                x = intTree.getValue();
                stringBuilder.append(x);
                stringBuilder.append("\n");
                indexValue.put(intTree.getKey(), -1);
            }

            fr.write(stringBuilder.toString());
            fr.flush();

            integers.clear();
        }

        stopReadFile:

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

