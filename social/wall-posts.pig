/*
 * load the link dataset, consisting of two columns of user id's
 * each row represents a link between the users.
 *
 * user_a | user_b
 * 1      | 4
 * 1      | 6
 * 1      | 9
 * ...
 *
 */
LINKS = LOAD 'user-links-small.txt.gz' as (user_a: int, user_b: int);
-- extract the first column of the LINKS
LINKS_COL1 = FOREACH LINKS GENERATE FLATTEN(user_a);
/* group the column by its first and only ($0) column.
 * each group is a user id, and the corresponding bag
 * consists of all the user id's of that group present
 * in the first column. The number of the id's in the bag
 * is the number of time the group is present in the column.
 * For example, user id 1, and user id 2 are both presented
 * 10 times.
 *
 * group | bag
 * 1     | (1,1,1,1,1,1,1,1,1,1)
 * 2     | (2,2,2,2,2,2,2,2,2,2)
 * ...
 */
GROUPED = GROUP LINKS_COL1 by $0;
/*
 * loop through the groups, and for each group, generate a row
 * consisting of the group, and the size of the bag.
 *
 * user_id | friend_count
 * 1       | 10
 * 2       | 10
 * ...
 */
ID_COUNT = FOREACH GROUPED GENERATE group as user_id, COUNT(LINKS_COL1) as friend_count;

/*
 * Load the post dataset, consisting of three columns: two user id's and a time.
 * The time column is ignored in this script. Each row represents a post
 * made by the user with user_id of column 1, two the wall of the user with
 * user_id 2.
 *
 * user_a | user_b | time
 * 1      | 9      | 1178750526
 * 1      | 21     | 1165506888
 * 1      | 21     | 1197223661
 * ...
 */
POSTS = LOAD 'user-wall-small.txt.gz' as (user_a: int, user_b: int, time: int);
/*
 * The next three lines do the same grouping as shown above.
 */
POSTS_COL1 = FOREACH POSTS GENERATE FLATTEN(user_a);
GROUPED = GROUP POSTS_COL1 by $0;
/*
 * Each row of the two column data ID_POSTS presents the number of posts
 * at column 2, done by the user with user_id of column 1.
 */
ID_POSTS = FOREACH GROUPED GENERATE group as user_id, COUNT(POSTS_COL1) as post_count;
/*
 * We are interested in the average of the posts, so we also need
 * to consider the users who have done 0 posts. The post data
 * does not explicitly contain these numbers, so we need to generate
 * those rows, and join them to the ID_POSTS.
 *
 * Co-group the two data created above, to get the number of links
 * and number of posts for each user.
 */
GROUPED = COGROUP ID_COUNT BY user_id, ID_POSTS BY user_id;
/*
 * From the group, filter those groups that do not have
 * any posts.
 */
ZEROS   = FILTER GROUPED BY IsEmpty(ID_POSTS);
/*
 * For each user with no posts, generate a row with the user_id and
 * long 0.
 */
ZERO_ID_POSTS = FOREACH ZEROS GENERATE group as user_id, (long)0 as post_count;
/*
 * Create a union of the data, to get the 0 posts and >0 posts to the
 * same variable
 */
ID_POSTS = UNION ID_POSTS, ZERO_ID_POSTS;
/*
 * Now, join the two data, to get the number of links and number of
 * posts for each user in the same variable.
 */
ID_COUNT_POSTS = JOIN ID_COUNT BY user_id, ID_POSTS BY user_id;
-- group the joined data by the link count.
GROUPED = GROUP ID_COUNT_POSTS BY $1;
/*
 * For each of the links counts, generate a row with the
 * link count, and the average of the posts done by the users
 * with that number of links, by using AVG.
 *
 * num_links | num_posts
 * 1         | 0.29158754272692744
 * 2         | 0.5536440412887081
 * 3         | 0.8436559139784946
 * ....
 */
FRIEND_AVG_POSTS = FOREACH GROUPED GENERATE group, AVG(ID_COUNT_POSTS.$3);
/*
 * Finally order the data by the number of links.
 */
ORDERED_FRIEND_AVG_POSTS = ORDER FRIEND_AVG_POSTS BY $0 ASC;
-- Store the data in the file 'question-2-output'
STORE ORDERED_FRIEND_AVG_POSTS INTO 'question-2-output';