package com.jimmy.groot.engine.data.lsm;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jimmy.groot.engine.exception.EngineException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Slf4j
public class SsTable implements Closeable {

    private ObjectMapper objectMapper;

    private RandomAccessFile tableFile;

    private TableMetaData tableMetaData;

    private TreeMap<String, Position> sparseIndex;

    public SsTable() {

    }

    static SsTable restore(String filePath, ObjectMapper objectMapper) {
        SsTable ssTable = build(filePath, 0L, objectMapper);
        ssTable.restoreFromFile();
        return ssTable;
    }

    static SsTable build(String filePath, Long partSize, ObjectMapper objectMapper) {
        SsTable ssTable = new SsTable();

        try {
            ssTable.tableFile = new RandomAccessFile(filePath, "rw");
            ssTable.tableFile.seek(0);
        } catch (Exception e) {
            log.error("创建ssTable文件失败", e);
            throw new EngineException("创建ssTable文件失败");
        }

        ssTable.objectMapper = objectMapper;
        ssTable.sparseIndex = new TreeMap<>();
        ssTable.tableMetaData = new TableMetaData();
        ssTable.tableMetaData.setPartSize(partSize);
        return ssTable;
    }

    long count() {
        return tableMetaData.getTotal();
    }

    TreeMap<String, TableData> load() {
        TreeMap<String, TableData> data = new TreeMap<>();

        try {
            for (Map.Entry<String, Position> stringPositionEntry : sparseIndex.entrySet()) {
                Position value = stringPositionEntry.getValue();

                long start = value.getStart();
                long len = value.getLen();

                byte[] indexBytes = new byte[(int) len];
                tableFile.seek(start);
                tableFile.read(indexBytes);

                String dataStr = new String(indexBytes, StandardCharsets.UTF_8);
                data.putAll(objectMapper.readValue(dataStr, new TypeReference<TreeMap<String, TableData>>() {
                }));
            }

            return data;
        } catch (Exception e) {
            log.error("读取ssTable数据失败", e);
            throw new EngineException("读取ssTable数据失败");
        }
    }

    void write(TreeMap<String, TableData> data) {
        Map<String, TableData> part = Maps.newHashMap();

        try {
            for (Map.Entry<String, TableData> entry : data.entrySet()) {
                String key = entry.getKey();
                TableData value = entry.getValue();

                part.put(key, value);

                //达到分段数量，开始写入数据段
                if (part.size() >= tableMetaData.getPartSize()) {
                    writeDataPart(part);
                }
            }
            //遍历完之后如果有剩余的数据（尾部数据不一定达到分段条件）写入文件
            if (MapUtil.isNotEmpty(part)) {
                writeDataPart(part);
            }

            long dataPartLen = tableFile.getFilePointer() - tableMetaData.getDataStart();
            tableMetaData.setDataLen(dataPartLen);
            //保存稀疏索引
            byte[] indexBytes = objectMapper.writeValueAsString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            tableMetaData.setIndexStart(tableFile.getFilePointer());
            tableFile.write(indexBytes);
            tableMetaData.setTotal(data.size());
            tableMetaData.setIndexLen(indexBytes.length);
            //保存文件索引
            tableMetaData.writeToFile(tableFile);
        } catch (Exception e) {
            log.error("写入ssTable数据失败", e);
            throw new EngineException("写入ssTable数据失败");
        }
    }

    TableData query(String key) {
        try {
            LinkedList<Position> sparseKeyPositionList = new LinkedList<>();

            Position lastSmallPosition = null;
            Position firstBigPosition = null;
            //从稀疏索引中找到最后一个小于key的位置，以及第一个大于key的位置
            for (String k : sparseIndex.keySet()) {
                if (k.compareTo(key) <= 0) {
                    lastSmallPosition = sparseIndex.get(k);
                } else {
                    firstBigPosition = sparseIndex.get(k);
                    break;
                }
            }

            if (lastSmallPosition != null) {
                sparseKeyPositionList.add(lastSmallPosition);
            }

            if (firstBigPosition != null) {
                sparseKeyPositionList.add(firstBigPosition);
            }

            if (CollUtil.isEmpty(sparseKeyPositionList)) {
                return null;
            }

            Position firstKeyPosition = sparseKeyPositionList.getFirst();
            Position lastKeyPosition = sparseKeyPositionList.getLast();

            long start = firstKeyPosition.getStart();
            long len = firstKeyPosition.equals(lastKeyPosition) ? firstKeyPosition.getLen() : lastKeyPosition.getStart() + lastKeyPosition.getLen() - start;
            //key如果存在必定位于区间内，所以只需要读取区间内的数据，减少io
            byte[] dataPart = new byte[(int) len];

            tableFile.seek(start);
            tableFile.read(dataPart);

            int pStart = 0;
            //读取分区数据
            for (Position position : sparseKeyPositionList) {
                JsonNode jsonNode = objectMapper.readTree(new String(dataPart, pStart, (int) position.getLen()));

                if (jsonNode.has(key)) {
                    JsonNode js = jsonNode.get(key);

                    if (js != null) {
                        return objectMapper.convertValue(js, TableData.class);
                    }
                }

                pStart += (int) position.getLen();
            }

            return null;
        } catch (Exception e) {
            log.error("获取ssTable数据失败", e);
            throw new EngineException("获取ssTable数据失败");
        }
    }


    @Override
    public void close() throws IOException {
        tableFile.close();
    }

    /**
     * 从文件中恢复ssTable到内存
     */
    private void restoreFromFile() {
        try {
            //先读取索引
            TableMetaData tableMetaData = TableMetaData.readFromFile(tableFile);
            //读取稀疏索引
            byte[] indexBytes = new byte[(int) tableMetaData.getIndexLen()];
            tableFile.seek(tableMetaData.getIndexStart());
            tableFile.read(indexBytes);
            String indexStr = new String(indexBytes, StandardCharsets.UTF_8);

            this.sparseIndex = objectMapper.readValue(indexStr,
                    new TypeReference<TreeMap<String, Position>>() {
                    });

            this.tableMetaData = tableMetaData;
        } catch (Exception e) {
            log.error("恢复ssTable失败", e);
            throw new EngineException("恢复ssTable失败");
        }
    }

    /**
     * 将数据分区写入文件
     *
     * @param partData
     * @throws IOException
     */
    private void writeDataPart(Map<String, TableData> partData) throws IOException {
        byte[] partDataBytes = objectMapper.writeValueAsString(partData).getBytes(StandardCharsets.UTF_8);
        long start = tableFile.getFilePointer();
        tableFile.write(partDataBytes);

        //记录数据段的第一个key到稀疏索引中
        Optional<String> firstKey = partData.keySet().stream().findFirst();
        firstKey.ifPresent(s -> sparseIndex.put(s, new Position(start, partDataBytes.length)));
        partData.clear();
    }

    @Data
    public static class Position implements Serializable {

        private long start;

        private long len;

        public Position(long start, long len) {
            this.start = start;
            this.len = len;
        }
    }
}
