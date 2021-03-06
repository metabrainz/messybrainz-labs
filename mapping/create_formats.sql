BEGIN;
DROP TABLE mapping.format_sort;
CREATE TABLE mapping.format_sort ( format integer, sort integer );
INSERT INTO mapping.format_sort values (1, 1);
INSERT INTO mapping.format_sort values (2, 3);
INSERT INTO mapping.format_sort values (3, 6);
INSERT INTO mapping.format_sort values (4, 11);
INSERT INTO mapping.format_sort values (5, 12);
INSERT INTO mapping.format_sort values (6, 16);
INSERT INTO mapping.format_sort values (7, 25);
INSERT INTO mapping.format_sort values (8, 26);
INSERT INTO mapping.format_sort values (9, 27);
INSERT INTO mapping.format_sort values (10, 28);
INSERT INTO mapping.format_sort values (11, 33);
INSERT INTO mapping.format_sort values (12, 34);
INSERT INTO mapping.format_sort values (13, 35);
INSERT INTO mapping.format_sort values (14, 36);
INSERT INTO mapping.format_sort values (15, 37);
INSERT INTO mapping.format_sort values (16, 38);
INSERT INTO mapping.format_sort values (17, 39);
INSERT INTO mapping.format_sort values (18, 40);
INSERT INTO mapping.format_sort values (19, 42);
INSERT INTO mapping.format_sort values (20, 43);
INSERT INTO mapping.format_sort values (21, 44);
INSERT INTO mapping.format_sort values (22, 45);
INSERT INTO mapping.format_sort values (23, 46);
INSERT INTO mapping.format_sort values (24, 49);
INSERT INTO mapping.format_sort values (25, 57);
INSERT INTO mapping.format_sort values (26, 60);
INSERT INTO mapping.format_sort values (27, 61);
INSERT INTO mapping.format_sort values (28, 62);
INSERT INTO mapping.format_sort values (29, 63);
INSERT INTO mapping.format_sort values (30, 64);
INSERT INTO mapping.format_sort values (31, 74);
INSERT INTO mapping.format_sort values (32, 75);
INSERT INTO mapping.format_sort values (33, 76);
INSERT INTO mapping.format_sort values (34, 77);
INSERT INTO mapping.format_sort values (35, 82);
INSERT INTO mapping.format_sort values (36, 48);
INSERT INTO mapping.format_sort values (37, 2);
INSERT INTO mapping.format_sort values (38, 4);
INSERT INTO mapping.format_sort values (39, 5);
INSERT INTO mapping.format_sort values (40, 71);
INSERT INTO mapping.format_sort values (41, 72);
INSERT INTO mapping.format_sort values (42, 17);
INSERT INTO mapping.format_sort values (43, 18);
INSERT INTO mapping.format_sort values (44, 19);
INSERT INTO mapping.format_sort values (45, 20);
INSERT INTO mapping.format_sort values (46, 22);
INSERT INTO mapping.format_sort values (47, 23);
INSERT INTO mapping.format_sort values (48, 41);
INSERT INTO mapping.format_sort values (49, 47);
INSERT INTO mapping.format_sort values (50, 59);
INSERT INTO mapping.format_sort values (51, 66);
INSERT INTO mapping.format_sort values (52, 65);
INSERT INTO mapping.format_sort values (53, 67);
INSERT INTO mapping.format_sort values (54, 68);
INSERT INTO mapping.format_sort values (55, 69);
INSERT INTO mapping.format_sort values (56, 70);
INSERT INTO mapping.format_sort values (57, 80);
INSERT INTO mapping.format_sort values (58, 79);
INSERT INTO mapping.format_sort values (59, 7);
INSERT INTO mapping.format_sort values (60, 29);
INSERT INTO mapping.format_sort values (61, 30);
INSERT INTO mapping.format_sort values (62, 31);
INSERT INTO mapping.format_sort values (63, 10);
INSERT INTO mapping.format_sort values (64, 8);
INSERT INTO mapping.format_sort values (65, 9);
INSERT INTO mapping.format_sort values (66, 78);
INSERT INTO mapping.format_sort values (67, 13);
INSERT INTO mapping.format_sort values (68, 14);
INSERT INTO mapping.format_sort values (69, 15);
INSERT INTO mapping.format_sort values (70, 81);
INSERT INTO mapping.format_sort values (71, 21);
INSERT INTO mapping.format_sort values (72, 24);
INSERT INTO mapping.format_sort values (73, 50);
INSERT INTO mapping.format_sort values (74, 51);
INSERT INTO mapping.format_sort values (75, 52);
INSERT INTO mapping.format_sort values (76, 53);
INSERT INTO mapping.format_sort values (77, 54);
INSERT INTO mapping.format_sort values (78, 55);
INSERT INTO mapping.format_sort values (79, 56);
INSERT INTO mapping.format_sort values (80, 58);
INSERT INTO mapping.format_sort values (81, 73);
CREATE INDEX format_sort_format_ndx ON mapping.format_sort(format);
CREATE INDEX format_sort_sort_ndx ON mapping.format_sort(sort);
COMMIT;
