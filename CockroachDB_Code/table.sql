USE defaultdb;

CREATE TABLE user_review (
    rating INT,
    title TEXT,
    text TEXT,
    asin TEXT,
    parent_asin TEXT,
    user_id TEXT,
    timestamp TEXT,
    helpful_vote INT,
    verified_purchase BOOL
);

SELECT username FROM system.users;

-- Step 3: Verify the data was inserted correctly
SELECT * FROM user_review;