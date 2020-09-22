package domain;

import lombok.Data;

import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class UserBehavior {

    private String dateStr;
    private long userId;
    private long itemId;
    private int categoryId;
    private String behavior;
    private long timestamp;

    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public UserBehavior() {}

    public UserBehavior(long _userId, long _itemId, int _categoryId, String _behavior, long _timestamp) {
        this.userId = _userId;
        this.itemId = _itemId;
        this.categoryId = _categoryId;
        this.behavior = _behavior;
        this.timestamp = _timestamp;
        this.dateStr = df.format(new Date(this.timestamp * 1000));
    }
}