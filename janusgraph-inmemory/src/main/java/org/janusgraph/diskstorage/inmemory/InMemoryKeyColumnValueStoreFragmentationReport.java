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

import java.util.List;

public class InMemoryKeyColumnValueStoreFragmentationReport {
    private String name;
    private int numStores;
    private int numMultipageStores;
    private int totalPageCount;
    private int[] pageLevels;
    private int[] pageCounts;
    private int[] compressablePageLevels;
    private int[] compressablePageCounts;
    private int[] reductionLevels;
    private int[] reductionCounts;
    private int[] entryLevels;
    private int[] entryCounts;
    private int keysByteSize;
    private int numFragmentedStores;
    private int totalFragmentedPages;
    private int numCompressableStores;
    private int totalCompressablePages;
    private int totalAchievablePageReduction;
    private List<InMemoryColumnValueStore> storesToDefragment;

    private InMemoryKeyColumnValueStoreFragmentationReport(Builder builder) {
        setName(builder.name);
        setNumStores(builder.numStores);
        setNumMultipageStores(builder.numMultipageStores);
        setTotalPageCount(builder.totalPageCount);
        setPageLevels(builder.pageLevels);
        setPageCounts(builder.pageCounts);
        setCompressablePageLevels(builder.compressablePageLevels);
        setCompressablePageCounts(builder.compressablePageCounts);
        setReductionLevels(builder.reductionLevels);
        setReductionCounts(builder.reductionCounts);
        setEntryLevels(builder.entryLevels);
        setEntryCounts(builder.entryCounts);
        setKeysByteSize(builder.keysByteSize);
        setNumFragmentedStores(builder.numFragmentedStores);
        setTotalFragmentedPages(builder.totalFragmentedPages);
        setNumCompressableStores(builder.numCompressableStores);
        setTotalCompressablePages(builder.totalCompressablePages);
        setTotalAchievablePageReduction(builder.totalAchievablePageReduction);
        setStoresToDefragment(builder.storesToDefragment);
    }

    public List<InMemoryColumnValueStore> getStoresToDefragment() {
        return storesToDefragment;
    }

    public void setStoresToDefragment(List<InMemoryColumnValueStore> storesToDefragment) {
        this.storesToDefragment = storesToDefragment;
    }

    public int getTotalAchievablePageReduction() {
        return totalAchievablePageReduction;
    }

    public void setTotalAchievablePageReduction(int totalAchievablePageReduction) {
        this.totalAchievablePageReduction = totalAchievablePageReduction;
    }

    public int[] getReductionCounts() {
        return reductionCounts;
    }

    public void setReductionCounts(int[] reductionCounts) {
        this.reductionCounts = reductionCounts;
    }

    public int[] getReductionLevels() {
        return reductionLevels;
    }

    public void setReductionLevels(int[] reductionLevels) {
        this.reductionLevels = reductionLevels;
    }

    public int getNumMultipageStores() {
        return numMultipageStores;
    }

    public void setNumMultipageStores(int numMultipageStores) {
        this.numMultipageStores = numMultipageStores;
    }

    public int getNumStores() {
        return numStores;
    }

    public void setNumStores(int numStores) {
        this.numStores = numStores;
    }

    public String getName() {
        return name;
    }

    public void setName(String storeName) {
        this.name = storeName;
    }

    public int getTotalPageCount() {
        return totalPageCount;
    }

    public void setTotalPageCount(int totalPageCount) {
        this.totalPageCount = totalPageCount;
    }

    public int[] getPageLevels() {
        return pageLevels;
    }

    public void setPageLevels(int[] pageLevels) {
        this.pageLevels = pageLevels;
    }

    public int[] getPageCounts() {
        return pageCounts;
    }

    public void setPageCounts(int[] pageCounts) {
        this.pageCounts = pageCounts;
    }

    public int[] getCompressablePageLevels() {
        return compressablePageLevels;
    }

    public void setCompressablePageLevels(int[] compressablePageLevels) {
        this.compressablePageLevels = compressablePageLevels;
    }

    public int[] getCompressablePageCounts() {
        return compressablePageCounts;
    }

    public void setCompressablePageCounts(int[] compressablePageCounts) {
        this.compressablePageCounts = compressablePageCounts;
    }

    public int[] getEntryLevels() {
        return entryLevels;
    }

    public void setEntryLevels(int[] entryLevels) {
        this.entryLevels = entryLevels;
    }

    public int[] getEntryCounts() {
        return entryCounts;
    }

    public void setEntryCounts(int[] entryCounts) {
        this.entryCounts = entryCounts;
    }

    public int getKeysByteSize() {
        return keysByteSize;
    }

    public void setKeysByteSize(int keysByteSize) {
        this.keysByteSize = keysByteSize;
    }

    public int getNumFragmentedStores() {
        return numFragmentedStores;
    }

    public void setNumFragmentedStores(int numFragmentedStores) {
        this.numFragmentedStores = numFragmentedStores;
    }

    public int getTotalFragmentedPages() {
        return totalFragmentedPages;
    }

    public void setTotalFragmentedPages(int totalFragmentedPages) {
        this.totalFragmentedPages = totalFragmentedPages;
    }

    public int getNumCompressableStores() {
        return numCompressableStores;
    }

    public void setNumCompressableStores(int numCompressableStores) {
        this.numCompressableStores = numCompressableStores;
    }

    public int getTotalCompressablePages() {
        return totalCompressablePages;
    }

    public void setTotalCompressablePages(int totalCompressablePages) {
        this.totalCompressablePages = totalCompressablePages;
    }

    private void printHistogram(StringBuilder sb, int[] levels, int[] counts) {
        int i;
        for (i = 0; i < levels.length; i++) {
            if (counts[i] > 0) {
                sb.append("<=").append(levels[i]).append(": ").append(counts[i]).append("; ");
            }
        }
        if (counts[i] > 0) {
            sb.append(">").append(levels[i - 1]).append(": ").append(counts[i]).append("; ")
                .append(System.lineSeparator());
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(System.lineSeparator());

        sb.append("KCVS name: ").append(name).append(System.lineSeparator());
        sb.append("Total number of CVS: ").append(numStores).append(System.lineSeparator());
        sb.append("Total number of multi-page CVS: ").append(numMultipageStores).append(System.lineSeparator());
        sb.append("Total number of pages: ").append(totalPageCount).append(System.lineSeparator());
        sb.append(System.lineSeparator());
        sb.append("Number of fragmented CVS: ").append(numFragmentedStores).append(System.lineSeparator());
        sb.append("Total number of fragmented pages: ").append(totalFragmentedPages).append(System.lineSeparator());
        sb.append(System.lineSeparator());
        sb.append("Number of compressible CVS: ").append(numCompressableStores).append(System.lineSeparator());
        sb.append("Total number of compressible pages: ").append(totalCompressablePages).append(System.lineSeparator());
        sb.append("Total achievable page reduction: ").append(totalAchievablePageReduction).append(System.lineSeparator());
        sb.append(System.lineSeparator());
        sb.append("Total row keys bytesize: ").append(keysByteSize).append(System.lineSeparator());
        sb.append(System.lineSeparator());

        sb.append("Number of CVS by #entries: ").append(System.lineSeparator());
        printHistogram(sb, entryLevels, entryCounts);
        sb.append(System.lineSeparator());

        sb.append("Number of CVS by #pages: ").append(System.lineSeparator());
        printHistogram(sb, pageLevels, pageCounts);
        sb.append(System.lineSeparator());

        sb.append("Number of CVS by #compressable pages: ").append(System.lineSeparator());
        printHistogram(sb, compressablePageLevels, compressablePageCounts);
        sb.append(System.lineSeparator());

        sb.append("Number of CVS by achievable page reduction: ").append(System.lineSeparator());
        printHistogram(sb, reductionLevels, reductionCounts);
        sb.append(System.lineSeparator());

        return sb.toString();
    }

    public static final class Builder {
        private String name;
        private int numStores;
        private int numMultipageStores;
        private int totalPageCount;
        private int[] pageLevels;
        private int[] pageCounts;
        private int[] compressablePageLevels;
        private int[] compressablePageCounts;
        private int[] reductionLevels;
        private int[] reductionCounts;
        private int[] entryLevels;
        private int[] entryCounts;
        private int keysByteSize;
        private int numFragmentedStores;
        private int totalFragmentedPages;
        private int numCompressableStores;
        private int totalCompressablePages;
        private int totalAchievablePageReduction;
        private List<InMemoryColumnValueStore> storesToDefragment;

        public Builder name(String val) {
            name = val;
            return this;
        }

        public Builder numStores(int val) {
            numStores = val;
            return this;
        }

        public Builder numMultipageStores(int val) {
            numMultipageStores = val;
            return this;
        }

        public Builder totalPageCount(int val) {
            totalPageCount = val;
            return this;
        }

        public Builder pageLevels(int[] val) {
            pageLevels = val;
            return this;
        }

        public Builder pageCounts(int[] val) {
            pageCounts = val;
            return this;
        }

        public Builder compressablePageLevels(int[] val) {
            compressablePageLevels = val;
            return this;
        }

        public Builder compressablePageCounts(int[] val) {
            compressablePageCounts = val;
            return this;
        }

        public Builder reductionLevels(int[] val) {
            reductionLevels = val;
            return this;
        }

        public Builder reductionCounts(int[] val) {
            reductionCounts = val;
            return this;
        }

        public Builder entryLevels(int[] val) {
            entryLevels = val;
            return this;
        }

        public Builder entryCounts(int[] val) {
            entryCounts = val;
            return this;
        }

        public Builder keysByteSize(int val) {
            keysByteSize = val;
            return this;
        }

        public Builder numFragmentedStores(int val) {
            numFragmentedStores = val;
            return this;
        }

        public Builder totalFragmentedPages(int val) {
            totalFragmentedPages = val;
            return this;
        }

        public Builder numCompressableStores(int val) {
            numCompressableStores = val;
            return this;
        }

        public Builder totalCompressablePages(int val) {
            totalCompressablePages = val;
            return this;
        }

        public Builder totalAchievablePageReduction(int val) {
            totalAchievablePageReduction = val;
            return this;
        }

        public Builder storesToDefragment(List<InMemoryColumnValueStore> val) {
            storesToDefragment = val;
            return this;
        }

        public InMemoryKeyColumnValueStoreFragmentationReport build() {
            return new InMemoryKeyColumnValueStoreFragmentationReport(this);
        }
    }
}
