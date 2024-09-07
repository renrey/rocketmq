/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.logfile;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class DefaultMappedFile extends AbstractMappedFile {
    // start -- 静态遍历 共享的
    public static final int OS_PAGE_SIZE = 1024 * 4;// 默认当前os的page （cache）大小：4kb
    public static final Unsafe UNSAFE = getUnsafe();
    private static final Method IS_LOADED_METHOD;
    public static final int UNSAFE_PAGE_SIZE = UNSAFE == null ? OS_PAGE_SIZE : UNSAFE.pageSize();

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);// 统计当前进程 mmap文件 使用了多少内存空间

    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);// 统计mmap多少文件

    // 3种Position的原子更新类
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;
    // end --- 静态

    protected volatile int wrotePosition;// 已经写入的位置
    protected volatile int committedPosition;// 已经commit的位置
    protected volatile int flushedPosition;// 已经刷盘的文件位置
    protected int fileSize;// 文件大小
    protected FileChannel fileChannel;// 文件管道-》用于触发写入等操作，
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;// 写入数据堆内存buffer，用于触发fileChannel的写入，commit才清空，即暂时不写入page cache的内存
    // 但是writeBuffer在commit后，就不再使用写入 -》即只有文件第一次写入使用commit后不使用
    protected TransientStorePool transientStorePool = null;
    protected String fileName;
    protected long fileFromOffset;
    protected File file;
    protected MappedByteBuffer mappedByteBuffer;// mmap的，用于写入数据时，直接write堆外内存
    protected volatile long storeTimestamp = 0;
    protected boolean firstCreateInQueue = false;
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTime = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of first message referenced
     * by this logical queue.
     */
    private long startTimestamp = -1;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of last message referenced
     * by this logical queue.
     */
    private long stopTimestamp = -1;

    static {
        // 这3个是3种Position的原子更新类
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");

        Method isLoaded0method = null;
        // On the windows platform and openjdk 11 method isLoaded0 always returns false.
        // see https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/java.base/windows/native/libnio/MappedByteBuffer.c#L34
        if (!SystemUtils.IS_OS_WINDOWS) {
            try {
                isLoaded0method = MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
                isLoaded0method.setAccessible(true);
            } catch (NoSuchMethodException ignore) {
            }
        }
        IS_LOADED_METHOD = isLoaded0method;
    }

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    @Override
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        // 1. 创建文件管道，mmap 映射
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();// 初始从transientStorePool申请内存块
        this.transientStorePool = transientStorePool;// 用于存储使用的临时内存池
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);

        // 文件名转成
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        UtilAll.ensureDirOK(this.file.getParent());

        try {
            // 1. 目标文件的管道创建
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 2. mmap 映射 目标文件
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 直接加入文件大小
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();// 文件数+1
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    @Override
    public boolean renameTo(String fileName) {
        File newFile = new File(fileName);
        boolean rename = file.renameTo(newFile);
        if (rename) {
            this.fileName = fileName;
            this.file = newFile;
        }
        return rename;
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }

        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return false;
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb) {
        assert byteBufferMsg != null;
        assert cb != null;
        // 获取已写入的位置-》本次写入开始的位置
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos < this.fileSize) {
            // 当前文件未写满

            // 如果当前有writeBuffer（中间缓存），则使用，否则就是mmap 的fd
            /**
             * 2种写缓存方式：writeBuffer-- 堆写缓存用于传统write，需要触发多一步写流 ， mmap--写堆外用于直接让os写文件
             * 但是writeBuffer只是文件第一次写使用的方式（未commit）
             * 触发commit后，后面都是使用mmap的方式
             *
             */
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);// 开始位置更新
            // 往byteBuffer 空间 写入内容（最大this.fileSize - currentPos-》即当前文件剩余空间）
            // 2种情况：有writeBuffer-> 写入中间缓存 ，mmap文件-> 直接写入文件（page cache）
            AppendMessageResult result = cb.doAppend(byteBuffer, this.fileFromOffset, this.fileSize - currentPos, byteBufferMsg);
            // 记录器加 本次写入大小
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();// 更新时间戳
            return result;
        }
        // 已写满了，但没重置已写入大小-》错误
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;

        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if (currentPos < this.fileSize) {
            // 文件未满，可写入

            // 获取writeBuffer 或者mmap的buffer
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;

            // 执行写入
            if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
                // 批量消息
                // traditional batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                    (MessageExtBatch) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBrokerInner) {
                // 单条消息
                // 新出的批量消息
                // traditional single message or newly introduced inner-batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                    (MessageExtBrokerInner) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        // 已满错误
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        // 返回writeBuffer，无则直接文件流
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    // 效果就是追加写
    @Override
    public boolean appendMessage(ByteBuffer data) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        int remaining = data.remaining();

        if ((currentPos + remaining) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                while (data.hasRemaining()) {
                    // write()写入文件fd（page）
                    this.fileChannel.write(data);
                }
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, remaining);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    @Override
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            // 可以进行flush（待flush的page数达到指定 参数、有可flush、已写满）
            if (this.hold()) {// 当前对象加锁，串行执行
                int value = getReadPosition();

                try {
                    this.mappedByteBufferAccessCountSinceLastSwap++;
                    // 执行fsync
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // 其实就是写数据到writeBuffer，就是传统write写入文件
                        // 有writeBuffer
                        // OR 对文件（非mmap映射fd）写入
                        this.fileChannel.force(false);// 对文件本身flush
                    } else {
                        // 没writeBuffer就是不用堆内存做缓冲，直接写堆外空间，所以flush mmap的fd
                        this.mappedByteBuffer.force();// 对mmap flush
                    }
                    this.lastFlushTime = System.currentTimeMillis();
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                FLUSHED_POSITION_UPDATER.set(this, value);// 更新已flush位置
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + FLUSHED_POSITION_UPDATER.get(this));
                FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    @Override
    public int commit(final int commitLeastPages) {
        // 无writeBuffer，commit无效-》需要申请writeBuffer空间，并写入
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return WROTE_POSITION_UPDATER.get(this);
        }

        //no need to commit data to file channel, so just set committedPosition to wrotePosition.
        if (transientStorePool != null && !transientStorePool.isRealCommit()) {
            COMMITTED_POSITION_UPDATER.set(this, WROTE_POSITION_UPDATER.get(this));
        } else if (this.isAbleToCommit(commitLeastPages)) {
            // 提交
            if (this.hold()) {// 同步方法并发控制commit进入
                // 执行commit-》写入到文件（本身fd，非mmap的）cache
                commit0();
                this.release();// 释放hold的计数
            } else {
                log.warn("in commit, hold failed, commit offset = " + COMMITTED_POSITION_UPDATER.get(this));
            }
        }

        // commit 到文件流完成后，清空buffer
        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
            // 把writeBuffer使用的内存块归还回池
            this.transientStorePool.returnBuffer(writeBuffer);// 归还
            this.writeBuffer = null;// 并且 清理buffer引用 -》好像后面再也没有申请的地方
        }

        return COMMITTED_POSITION_UPDATER.get(this);
    }

    protected void commit0() {
        int writePos = WROTE_POSITION_UPDATER.get(this);
        int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);

        if (writePos - lastCommittedPosition > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);// 直接往文件的fd （不是mmap的）写入page cache
                COMMITTED_POSITION_UPDATER.set(this, writePos);// 更新commit位置
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = FLUSHED_POSITION_UPDATER.get(this);
        int write = getReadPosition();

        // 当前文件内容已写满-》可flush
        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            // 需要flush page数达到 参数，返回true
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        // 没配置需要最少flush 页数，则看是否已经超过flush位置-》有可flush的
        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = COMMITTED_POSITION_UPDATER.get(this);
        int write = WROTE_POSITION_UPDATER.get(this);

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    @Override
    public int getFlushedPosition() {
        return FLUSHED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;

                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();// 申请一个byteBuffer的副本（具体指向区域同一个）
                byteBuffer.position(pos);// 更新开始为pos
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }
        // 清理空间
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        // 计数清理
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));// 已mmap空间
        TOTAL_MAPPED_FILES.decrementAndGet();// 文件数-1
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    @Override
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                long lastModified = getLastModifiedTimestamp();
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime)
                    + "," + (System.currentTimeMillis() - lastModified));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    @Override
    public int getWrotePosition() {
        return WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * @return The max position which have valid data
     */
    @Override
    public int getReadPosition() {
        return transientStorePool == null || !transientStorePool.isRealCommit() ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        long flush = 0;
        // long time = System.currentTimeMillis();
        // 等于遍历当前文件的每个 page cache-
        for (long i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            // mmap 的堆外内存的 每个page 设置第1个放0 -》在内存申请page
            byteBuffer.put((int) i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // if (j % 1000 == 0) {
            //     log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
            //     time = System.currentTimeMillis();
            //     try {
            //         Thread.sleep(0);
            //     } catch (InterruptedException e) {
            //         log.error("Interrupted", e);
            //     }
            // }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public boolean swapMap() {
        if (getRefCount() == 1 && this.mappedByteBufferWaitToClean == null) {

            if (!hold()) {
                log.warn("in swapMap, hold failed, fileName: " + this.fileName);
                return false;
            }
            try {
                this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                this.mappedByteBufferAccessCountSinceLastSwap = 0L;
                this.swapMapTime = System.currentTimeMillis();
                log.info("swap file " + this.fileName + " success.");
                return true;
            } catch (Exception e) {
                log.error("swapMap file " + this.fileName + " Failed. ", e);
            } finally {
                this.release();
            }
        } else {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
        }
        return false;
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public long getLastFlushTime() {
        return this.lastFlushTime;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public void renameToDelete() {
        //use Files.move
        if (!fileName.endsWith(".delete")) {
            String newFileName = this.fileName + ".delete";
            try {
                Path newFilePath = Paths.get(newFileName);
                // https://bugs.openjdk.org/browse/JDK-4724038
                // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
                // Windows can't move the file when mmapped.
                if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
                    long position = this.fileChannel.position();
                    UtilAll.cleanBuffer(this.mappedByteBuffer);
                    this.fileChannel.close();
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                    try (RandomAccessFile file = new RandomAccessFile(newFileName, "rw")) {
                        this.fileChannel = file.getChannel();
                        this.fileChannel.position(position);
                        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                    }
                } else {
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                }
                this.fileName = newFileName;
                this.file = new File(newFileName);
            } catch (IOException e) {
                log.error("move file {} failed", fileName, e);
            }
        }
    }

    @Override
    public void moveToParent() throws IOException {
        Path currentPath = Paths.get(fileName);
        String baseName = currentPath.getFileName().toString();
        Path parentPath = currentPath.getParent().getParent().resolve(baseName);
        // https://bugs.openjdk.org/browse/JDK-4724038
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
        // Windows can't move the file when mmapped.
        if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
            long position = this.fileChannel.position();
            UtilAll.cleanBuffer(this.mappedByteBuffer);
            this.fileChannel.close();
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
            try (RandomAccessFile file = new RandomAccessFile(parentPath.toFile(), "rw")) {
                this.fileChannel = file.getChannel();
                this.fileChannel.position(position);
                this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            }
        } else {
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
        }
        this.file = parentPath.toFile();
        this.fileName = parentPath.toString();
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }


    public Iterator<SelectMappedBufferResult> iterator(int startPos) {
        return new Itr(startPos);
    }

    public static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception ignore) {

        }
        return null;
    }

    public static long mappingAddr(long addr) {
        long offset = addr % UNSAFE_PAGE_SIZE;
        offset = (offset >= 0) ? offset : (UNSAFE_PAGE_SIZE + offset);
        return addr - offset;
    }

    public static int pageCount(long size) {
        return (int) (size + (long) UNSAFE_PAGE_SIZE - 1L) / UNSAFE_PAGE_SIZE;
    }

    @Override
    public boolean isLoaded(long position, int size) {
        if (IS_LOADED_METHOD == null) {
            return true;
        }
        try {
            long addr = ((DirectBuffer) mappedByteBuffer).address() + position;
            return (boolean) IS_LOADED_METHOD.invoke(mappedByteBuffer, mappingAddr(addr), size, pageCount(size));
        } catch (Exception e) {
            log.info("invoke isLoaded0 of file {} error:", file.getAbsolutePath(), e);
        }
        return true;
    }

    private class Itr implements Iterator<SelectMappedBufferResult> {
        private int start;
        private int current;
        private ByteBuffer buf;

        public Itr(int pos) {
            this.start = pos;
            this.current = pos;
            this.buf = mappedByteBuffer.slice();
            this.buf.position(start);
        }

        @Override
        public boolean hasNext() {
            return current < getReadPosition();
        }

        @Override
        public SelectMappedBufferResult next() {
            int readPosition = getReadPosition();
            if (current < readPosition && current >= 0) {
                if (hold()) {
                    ByteBuffer byteBuffer = buf.slice();
                    byteBuffer.position(current);
                    int size = byteBuffer.getInt(current);
                    ByteBuffer bufferResult = byteBuffer.slice();
                    bufferResult.limit(size);
                    current += size;
                    return new SelectMappedBufferResult(fileFromOffset + current, bufferResult, size,
                        DefaultMappedFile.this);
                }
            }
            return null;
        }

        @Override
        public void forEachRemaining(Consumer<? super SelectMappedBufferResult> action) {
            Iterator.super.forEachRemaining(action);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
