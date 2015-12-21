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
ID_COUNT = FOREACH GROUPED GENERATE group, COUNT(LINKS_COL1);
-- Extract the link counts
LINK_COUNTS = FOREACH ID_COUNT GENERATE FLATTEN($1);
/* group the column by its first ($0) column.
 * each group is a number of links, and the corresponding bag
 * consists of all the link counts of that group present
 * in the first column. The number of the links in the bag
 * is the number of time the group is present in the column.
 */
GROUPED = GROUP LINK_COUNTS by $0;
/*
 * For each of those groups, create a row with the number of
 * links, and the number of users having the number of links.
 */
DEGREE_COUNT = FOREACH GROUPED GENERATE group, COUNT(LINK_COUNTS);
-- order the data by the first column (degree)
H = ORDER DEGREE_COUNT BY $0 ASC;
-- store the data into 'question-1-output'
STORE H into 'question-1-output';