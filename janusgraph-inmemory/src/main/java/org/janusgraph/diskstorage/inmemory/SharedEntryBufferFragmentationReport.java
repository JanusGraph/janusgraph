// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.inmemory;

public class SharedEntryBufferFragmentationReport {
    private int pageCount;
    private int fragmentedPageCount;
    private int compressableChunksCount;
    private int compressablePageCount;
    private int achievablePageReduction;

    private SharedEntryBufferFragmentationReport(Builder builder) {
        setPageCount(builder.pageCount);
        setFragmentedPageCount(builder.fragmentedPageCount);
        setCompressableChunksCount(builder.compressableChunksCount);
        setCompressablePageCount(builder.compressablePageCount);
        setAchievablePageReduction(builder.achievablePageReduction);
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    public int getFragmentedPageCount() {
        return fragmentedPageCount;
    }

    public void setFragmentedPageCount(int fragmentedPageCount) {
        this.fragmentedPageCount = fragmentedPageCount;
    }

    public int getCompressableChunksCount() {
        return compressableChunksCount;
    }

    public void setCompressableChunksCount(int compressableChunksCount) {
        this.compressableChunksCount = compressableChunksCount;
    }

    public int getCompressablePageCount() {
        return compressablePageCount;
    }

    public void setCompressablePageCount(int compressablePageCount) {
        this.compressablePageCount = compressablePageCount;
    }

    public int getAchievablePageReduction() {
        return achievablePageReduction;
    }

    public void setAchievablePageReduction(int achievablePageReduction) {
        this.achievablePageReduction = achievablePageReduction;
    }

    public static final class Builder {
        private int pageCount;
        private int fragmentedPageCount;
        private int compressableChunksCount;
        private int compressablePageCount;
        private int achievablePageReduction;

        public Builder pageCount(int val) {
            pageCount = val;
            return this;
        }

        public Builder fragmentedPageCount(int val) {
            fragmentedPageCount = val;
            return this;
        }

        public Builder compressableChunksCount(int val) {
            compressableChunksCount = val;
            return this;
        }

        public Builder compressablePageCount(int val) {
            compressablePageCount = val;
            return this;
        }

        public Builder achievablePageReduction(int val) {
            achievablePageReduction = val;
            return this;
        }

        public SharedEntryBufferFragmentationReport build() {
            return new SharedEntryBufferFragmentationReport(this);
        }
    }
}
