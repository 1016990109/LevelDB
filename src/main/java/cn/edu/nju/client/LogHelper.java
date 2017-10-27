package cn.edu.nju.client;

import cn.edu.nju.MyProcessor;

import java.io.*;
import java.util.Map;
import java.util.UUID;

public class LogHelper {
    private String logFolderPath = "/opt/localdisk/";
    private String currentPath;
    private String flushPath;
    private MyProcessor processor;
    private ObjectOutputStream writer;
    private ObjectInputStream reader;
    private int maxCount;
    private int count = 0;

    public LogHelper(MyProcessor processor, int maxCount) {
        this.processor = processor;
        this.maxCount = maxCount;
    }

    public synchronized void writeLog(String key, Map<String, String> value) throws IOException {
        count++;

        ensureWriterOpen();
        writer.writeObject(key);
        writer.writeObject(value);
        writer.flush();

        if (count == maxCount) {
            refresh();
            count = 0;
        }
    }

    public synchronized void countIncrease() {
        count++;
    }

    /**
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void readLogs() throws IOException, ClassNotFoundException {
        File file = new File(logFolderPath);

        if (file.exists()) {
            File[] files = file.listFiles();
            for (File logFile: files) {
                if (!logFile.getPath().endsWith(".log"))
                    break;
                reader = new ObjectInputStream(new FileInputStream(logFile));

                try {
                    while (true) {
                        String key = (String) reader.readObject();
                        Map<String, String> value = (Map<String, String>) reader.readObject();

                        processor.put(key, value, false);
                    }
                } catch (EOFException e) {

                } finally {
                    reader.close();
                    currentPath = logFile.getPath();
                    flushPath = currentPath;
                    writer = new NoHeaderObjectOutputStream(new FileOutputStream(new File(currentPath), true));
                }
            }
        }
    }

    private void ensureWriterOpen() throws IOException {
        if (writer == null) {
            currentPath = getRandomFileName();
            File file = new File(currentPath);
            FileOutputStream fos = new FileOutputStream(file, true);
            if (file.length() > 0) {
                writer = new NoHeaderObjectOutputStream(fos);
            } else {
                writer = new ObjectOutputStream(fos);
            }
        }
    }

    /**
     * 换一个文件写
     * @throws IOException
     */
    public synchronized void refresh() throws IOException {
        writer.close();
        flushPath = currentPath;
        currentPath = getRandomFileName();
        writer = new ObjectOutputStream(new FileOutputStream(new File(currentPath)));
    }

    public synchronized void deleteLog() throws IOException {
        if (flushPath != null && !flushPath.equals("")) {
            File file = new File(flushPath);

            if (file.exists() && file.isFile()) {
                file.delete();
                flushPath = "";
            }
        }
    }

    private String getRandomFileName() {
        return logFolderPath + UUID.randomUUID() + ".log";
    }
}
