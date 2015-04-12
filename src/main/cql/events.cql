
CREATE KEYSPACE IF NOT EXISTS ks_events WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use ks_events;

CREATE TABLE IF NOT EXISTS events (
  evt_uid uuid,
  evt_id text,
  evt_value int,
  evt_spd int,
  evt_veh text,
  evt_gts timestamp,
  PRIMARY KEY(evt_uid)
);

CREATE TABLE IF NOT EXISTS events_byday (
  evt_year_gts text,
  evt_day_gts timestamp,
  nb_evts int,
  PRIMARY KEY(evt_year_gts,evt_day_gts)
);

CREATE TABLE IF NOT EXISTS events_byevtid (
  evt_id text,
  nb_evts int,
  PRIMARY KEY(evt_id)
);

CREATE TABLE IF NOT EXISTS events_byevtveh (
  evt_veh text,
  nb_evts int,
  PRIMARY KEY(evt_veh)
);

CREATE TABLE IF NOT EXISTS events_bydayevtid (
  evt_id text,
  nb_evts_byevtid int static,
  evt_day_gts timestamp,
  nb_evts int,
  PRIMARY KEY(evt_id,evt_day_gts)
);

CREATE TABLE IF NOT EXISTS events_bydayevtveh (
  evt_veh text,
  nb_evts_byevtveh int static,
  evt_day_gts timestamp,
  nb_evts int,
  PRIMARY KEY(evt_veh,evt_day_gts)
);

/*
INSERT INTO events (evt_uid,evt_id,evt_value,evt_spd,evt_veh,evt_gts)
VALUES (06049cbb-dfed-421f-b889-5f649a0de1ed,'A',23,78,'AZE','2013-05-02 10:30:29');
INSERT INTO events (evt_uid,evt_id,evt_value,evt_spd,evt_veh,evt_gts)
VALUES (522b1fe2-2e36-4cef-a667-cd4237d08b89,'A',43,984,'BHT','2013-05-03 08:50:12');
INSERT INTO events (evt_uid,evt_id,evt_value,evt_spd,evt_veh,evt_gts)
VALUES (9761d3d7-7fbd-4269-9988-6cfd4e188678,'B',13,234,'BHT','2013-05-03 12:40:23');
INSERT INTO events (evt_uid,evt_id,evt_value,evt_spd,evt_veh,evt_gts)
VALUES (9761d3d7-7fbd-4269-9988-6cfd4e188678,'B',28,197,'OIP','2013-05-03 09:23:45');
INSERT INTO events (evt_uid,evt_id,evt_value,evt_spd,evt_veh,evt_gts)
VALUES (b3a76c6b-7c7f-4af6-964f-803a9283c401,'B',33,98,'OIP','2013-05-04 11:30:12');

*/

//truncate events_byevtid;
//truncate events_byevtveh;

select * from events;
select * from events_byday;
select * from events_byevtveh;
select * from events_byevtid;
select * from events_bydayevtveh;
select * from events_bydayevtid;