-- Initial table for testing.
create table test (id INTEGER PRIMARY KEY, name VARCHAR(200), rec_date DATE, rec_time TIME, created_dt TIMESTAMP, num_times INTEGER DEFAULT 1);
insert into test(id, name, rec_date, rec_time, created_dt, num_times) values (10, 'Bruce Wayne', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435', 4);
insert into test(id, name, rec_date, rec_time, created_dt) values (20, 'Clark Kent', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435');
insert into test(id, name, rec_date, rec_time, created_dt, num_times) values (30, 'Steve Austin', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435', 1);
insert into test(id, name, rec_date, rec_time, created_dt, num_times) values (40, 'Steven Rogers', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435', 0);
insert into test(id, name, rec_date, rec_time, created_dt, num_times) values (50, 'Bruce Banner', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435', 7);
insert into test(id, name, rec_date, rec_time, created_dt, num_times) values (60, 'Linda Danvers', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435', 3);
insert into test(id, name, rec_date, rec_time, created_dt, num_times) values (70, 'Benjamin Grimm', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435', 2);
insert into test(id, name, rec_date, rec_time, created_dt, num_times) values (80, 'James Logan Howlett', '2012-07-01', '10:50:36', '2012-07-23 10:51:01.435', 1);
