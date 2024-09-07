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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);// 默认1
    protected volatile boolean available = true;// 默认true，SHUTDOWN变false
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

    // 同步方法（但只能串行hold，release可以并发执行）
    public synchronized boolean hold() {
        // 当前true
        if (this.isAvailable()) {
            // 执行refCount+1
            // 原来refCount>0,返回true
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                // 原来refCount = 0（当前1？） -1返回false -》大概就是当前资源不能使用，所以原来是0就false
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false; //
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        // refCount-1
        long value = this.refCount.decrementAndGet();
        // 原来就大于0，返回
        if (value > 0)
            return;
        // 原来=0（当前-1？），执行cleanup
        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
