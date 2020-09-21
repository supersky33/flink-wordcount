package domain;

import lombok.Data;

@Data
public class ItemViewCount {
    private long itemId;
    private long windowEnd;
    private long count;

    public ItemViewCount() {}

    public ItemViewCount(long _itemId, long _windowEnd, long _count) {
        this.itemId = _itemId;
        this.windowEnd = _windowEnd;
        this.count = _count;
    }
}
