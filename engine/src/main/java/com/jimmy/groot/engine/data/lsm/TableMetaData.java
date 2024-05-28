package com.jimmy.groot.engine.data.lsm;

import lombok.Data;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

public class TableMetaData implements Serializable {

    /**
     * 版本号
     */
    private long version;

    /**
     * 数据区开始
     */
    private long dataStart;

    /**
     * 数据区长度
     */
    private long dataLen;

    /**
     * 索引区开始
     */
    private long indexStart;

    /**
     * 索引区长度
     */
    private long indexLen;

    /**
     * 分段大小
     */
    private long partSize;

    TableMetaData() {

    }

    long getVersion() {
        return version;
    }

    void setVersion(long version) {
        this.version = version;
    }

    long getDataStart() {
        return dataStart;
    }

    void setDataStart(long dataStart) {
        this.dataStart = dataStart;
    }

    long getDataLen() {
        return dataLen;
    }

    void setDataLen(long dataLen) {
        this.dataLen = dataLen;
    }

    long getIndexStart() {
        return indexStart;
    }

    void setIndexStart(long indexStart) {
        this.indexStart = indexStart;
    }

    long getIndexLen() {
        return indexLen;
    }

    void setIndexLen(long indexLen) {
        this.indexLen = indexLen;
    }

    long getPartSize() {
        return partSize;
    }

    void setPartSize(long partSize) {
        this.partSize = partSize;
    }

    void writeToFile(RandomAccessFile file) throws IOException {
        file.writeLong(partSize);
        file.writeLong(dataStart);
        file.writeLong(dataLen);
        file.writeLong(indexStart);
        file.writeLong(indexLen);
        file.writeLong(version);
    }

    /**
     * 从文件中读取元信息，按照写入的顺序倒着读取出来
     *
     * @param file
     * @return
     */
    static TableMetaData readFromFile(RandomAccessFile file) throws IOException {
        TableMetaData tableMetaInfo = new TableMetaData();
        long fileLen = file.length();

        file.seek(fileLen - 8);
        tableMetaInfo.setVersion(file.readLong());

        file.seek(fileLen - 8 * 2);
        tableMetaInfo.setIndexLen(file.readLong());

        file.seek(fileLen - 8 * 3);
        tableMetaInfo.setIndexStart(file.readLong());

        file.seek(fileLen - 8 * 4);
        tableMetaInfo.setDataLen(file.readLong());

        file.seek(fileLen - 8 * 5);
        tableMetaInfo.setDataStart(file.readLong());

        file.seek(fileLen - 8 * 6);
        tableMetaInfo.setPartSize(file.readLong());

        return tableMetaInfo;
    }
}
