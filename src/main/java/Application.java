import javafx.beans.binding.StringBinding;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Application {
    private static Logger logger = Logger.getLogger(Application.class.getName());
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int FILE_SEGMENT_SIZE = 20000;
    private static final long FILE_SIZE = 147000;
    private static Random random = new Random();
    private static int j = 0;
    private static int i = 0;

    private static String fileName = "/data.txt";
    private static String pathOutFile =  "/out.txt";

    static URL location = Application.class.getProtectionDomain().getCodeSource().getLocation();
    private static File file = new File(location.getPath() + fileName);
    private static File out = new File(location.getPath() + pathOutFile);

    private static List<File> files = new ArrayList<>();
    private static List<File> filesSort = new ArrayList<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        createLargeFile();
        cutFile();
        sortFile();
        mergeFile();
    }

    static void createLargeFile() throws IOException {
        logger.info("Start process createLargeFile finish");

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
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

        FileChannel source = new FileInputStream(file).getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);

        File file = new File(location.getPath() + "/temp-" + i + ".txt");

        files.add(file);
        filesSort.add(new File(location.getPath() +"/sort-" + i + ".txt"));

        FileChannel destination = new FileOutputStream(file).getChannel();
        while ((source.read(buf)) != -1) {
            buf.flip();
            destination.write(buf);
            if (j == FILE_SEGMENT_SIZE) {
                i++;
                destination.close();

                file = new File(location.getPath() + "/temp-" + i + ".txt");
                files.add(file);
                filesSort.add(new File(location.getPath() +"/sort-" + i + ".txt"));
                destination = new FileOutputStream(file).getChannel();
                j = 0;
            } else {
                j++;
            }
            logger.info("create file: " + file.getName());
            buf.clear();
        }
        source.close();
        destination.close();
        logger.info("Finish process cutFile");
    }

    static void sortFile() throws IOException {
        logger.info("Start process sort by files");


        for (j = 0; j <= i; j++) {
            String line;
            List<Integer> integers = new ArrayList<>();
            FileInputStream fis = new FileInputStream(files.get(j));
            BufferedReader in = new BufferedReader(new InputStreamReader(fis));

            try {
                while ((line = in.readLine()) != null) {
                    if (!line.isEmpty()) integers.add(Integer.valueOf(line));
                }
            } catch (IOException e) {
                System.err.format("IOException: %s%n", e);
            } finally {
                fis.close();
                in.close();
            }

            logger.info("Sort file: " + files.get(j).getName());

            Collections.sort(integers);
            FileOutputStream fos = new FileOutputStream(filesSort.get(j));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fos));
            try {
                for (Integer itt : integers) {
                    out.write(itt + "\n");
                    out.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                fos.close();
                out.close();
            }
        }

        for (File file : files) {
            Files.deleteIfExists(file.toPath());
        }
        logger.info("Finish process sort by files");
    }

    private static void mergeFile() throws IOException {
        logger.info("Start process merge file");

        Map<Integer,Integer> indexValueTree;
        List<Integer> integers = new ArrayList<>();
        List<BufferedReader> brs = new ArrayList<>();
        Map<Integer,Integer> indexValue = new HashMap<>();

        for (j = 0; j <= i; j++) {
            brs.add(new BufferedReader(new FileReader(filesSort.get(j))));
            indexValue.put(j, -1);
        }

        boolean flag = true;
        FileWriter fr = new FileWriter(out, true);

        String value;
        int stopRead = 0;
        int x = 0;

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

        for (File file : filesSort) {
            Files.deleteIfExists(file.toPath());
        }

        fr.close();
        logger.info("Finish process merge file");
    }
}

