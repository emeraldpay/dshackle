/*
 * Copyright (c) 2016-2019 Igor Artamonov, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.ethereum.json;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonDeserialize(using = SyncingJsonDeserializer.class)
@JsonSerialize(using = SyncingJsonSerializer.class)
public class SyncingJson {

    private boolean syncing;

    private Long startingBlock;
    private Long currentBlock;
    private Long highestBlock;

    public boolean isSyncing() {
        return syncing;
    }

    public void setSyncing(boolean syncing) {
        this.syncing = syncing;
    }

    public Long getStartingBlock() {
        return startingBlock;
    }

    public void setStartingBlock(Long startingBlock) {
        this.startingBlock = startingBlock;
    }

    public Long getCurrentBlock() {
        return currentBlock;
    }

    public void setCurrentBlock(Long currentBlock) {
        this.currentBlock = currentBlock;
    }

    public Long getHighestBlock() {
        return highestBlock;
    }

    public void setHighestBlock(Long highestBlock) {
        this.highestBlock = highestBlock;
    }
}
