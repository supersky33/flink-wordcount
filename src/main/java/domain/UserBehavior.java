package domain;

import lombok.Data;

@Data
public class UserBehavior {
    private long userId;
    private long itemId;
    private int categoryId;
    private String behavior;
    private long timestamp;

    public UserBehavior() {}

    public UserBehavior(long _userId, long _itemId, int _categoryId, String _behavior, long _timestamp) {
        this.userId = _userId;
        this.itemId = _itemId;
        this.categoryId = _categoryId;
        this.behavior = _behavior;
        this.timestamp = _timestamp;
    }
}