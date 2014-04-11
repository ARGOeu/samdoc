DELIMITER //

USE `mrs`;

DROP PROCEDURE IF EXISTS loadmetricdatatospool;

CREATE PROCEDURE loadmetricdatatospool(in_serviceflavour VARCHAR(100), in_metricname VARCHAR(100), in_metricstatus VARCHAR(10), 
                 in_summarydata VARCHAR(255), in_detailsdata TEXT, in_voname VARCHAR(100), in_fqan VARCHAR(255), 
                 in_hostname VARCHAR(255), in_timestamp DATETIME, in_gatheredat VARCHAR(255))

BEGIN
  DECLARE v_gatheredat_id INT;
  DECLARE v_serviceflavour_name VARCHAR(100);
  DECLARE v_serviceflavour_id INT;
  DECLARE v_metricstatus_id INT;
  DECLARE v_metric_id INT;
  DECLARE v_fqan_id INT;
  DECLARE v_vo_id INT;
  DECLARE v_service_id INT;
  DECLARE v_metricdetail_id INT;
  DECLARE intFoundPos INT;
  DECLARE strElement VARCHAR(100);
  DECLARE v_check_time, v_insert_time INT;
  DECLARE v_metric_age_days INT;

  DECLARE done INT DEFAULT 0;
  DECLARE flavourCurs CURSOR FOR select name from tmpServiceFlavourList;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  set autocommit = 0;

  DROP TEMPORARY TABLE IF EXISTS tmpServiceFlavourList;
  CREATE TEMPORARY TABLE tmpServiceFlavourList ( `name` VARCHAR(50) NOT NULL DEFAULT '' ) ENGINE = MEMORY;

  SET intFoundPos = INSTR(in_serviceflavour,',');

  WHILE intFoundPos <> 0 do
    SET strElement = SUBSTRING(in_serviceflavour, 1, intFoundPos-1);
    SET in_serviceflavour = REPLACE(in_serviceflavour, CONCAT(strElement,','), '');
    INSERT INTO tmpServiceFlavourList (`name`) VALUES ( strElement);
    SET intFoundPos = INSTR(in_serviceflavour,',');
  END WHILE;

  IF in_serviceflavour <> '' THEN
    INSERT INTO tmpServiceFlavourList (`name`) VALUES (in_serviceflavour);
  END IF; 

  OPEN flavourCurs;

  REPEAT 
  
    set done = 0;
    FETCH flavourCurs INTO v_serviceflavour_name;

    IF NOT done THEN
      set v_service_id = NULL; 
      set v_metric_id = NULL; 
      /*Temporary hack - See: https://savannah.cern.ch/bugs/?62833*/ 
      set v_serviceflavour_name = IF(v_serviceflavour_name='sBDII', 'Site-BDII', v_serviceflavour_name); 
      select id into v_serviceflavour_id from service_type_flavour where lower(flavourname) = lower(v_serviceflavour_name);
      select id into v_metric_id from metric where lower(name) = lower(in_metricname) 
        and version = (select max(version) from metric where lower(name) = lower(in_metricname));
      select id into v_metricstatus_id from metricstatus where lower(description) = lower(in_metricstatus);
      select id into v_vo_id from vo where lower(voname) = lower(in_voname);
      select id into v_service_id from service where lower(hostname) = lower(in_hostname) and flavour_id = v_serviceflavour_id
                                       and id in (select distinct service_id from metrics_in_supported_services);
      select getOrAddFqan(in_fqan) into v_fqan_id;
      select getOrAddGatheredAt(in_gatheredat) into v_gatheredat_id;
      select UNIX_TIMESTAMP(CONVERT_TZ(in_timestamp, 'UTC', @@session.time_zone)) into v_check_time;
      select UNIX_TIMESTAMP() into v_insert_time;
      select floor((v_insert_time - v_check_time) / 86400) into v_metric_age_days;

      IF v_service_id is not NULL and v_metric_id is not NULL and v_metric_age_days < 7 THEN
        insert into metricdata_spool(gatheredat_id, metricstatus_id, detailsdata, metric_id, service_id,
                                       vo_id, fqan_id, summarydata, check_time, insert_time)
          VALUES (v_gatheredat_id, v_metricstatus_id, in_detailsdata, v_metric_id, v_service_id, v_vo_id, v_fqan_id, in_summarydata,
                  v_check_time, v_insert_time);
      ELSEIF (v_metric_age_days >= 7) THEN
      insert into metricdata_rejected(metricstatus, metric, service, service_flavour, vo, fqan, summarydata, gatheredat, check_time, insert_time, reason)
        VALUES (in_metricstatus, in_metricname, in_hostname, in_serviceflavour, in_voname, in_fqan, in_summarydata, in_gatheredat, v_check_time, v_insert_time, 'metric too old');
      ELSEIF (v_service_id is NULL and v_metric_id is NULL) THEN
        insert into metricdata_rejected(metricstatus, metric, service, service_flavour, vo, fqan, summarydata, gatheredat, check_time, insert_time, reason)
        VALUES (in_metricstatus, in_metricname, in_hostname, in_serviceflavour, in_voname, in_fqan, in_summarydata, in_gatheredat, v_check_time, v_insert_time, 'service_id and metric_id NULL');
      ELSEIF v_service_id is NULL THEN
        insert into metricdata_rejected(metricstatus, metric, service, service_flavour, vo, fqan, summarydata, gatheredat, check_time, insert_time, reason)
        VALUES (in_metricstatus, in_metricname, in_hostname, in_serviceflavour, in_voname, in_fqan, in_summarydata, in_gatheredat, v_check_time, v_insert_time, 'service_id NULL');
      ELSEIF v_metric_id is NULL THEN
        insert into metricdata_rejected(metricstatus, metric, service, service_flavour, vo, fqan, summarydata, gatheredat, check_time, insert_time, reason)
        VALUES (in_metricstatus, in_metricname, in_hostname, in_serviceflavour, in_voname, in_fqan, in_summarydata, in_gatheredat, v_check_time, v_insert_time, 'metric_id NULL');
      END IF;
      set done = 0; 
    END IF;

    commit; 

  UNTIL done END REPEAT;

  CLOSE flavourCurs;

END //

DROP PROCEDURE IF EXISTS insertRowToMetricData;

CREATE PROCEDURE insertRowToMetricData(in_service_id INT, in_metric_id INT, in_metricstatus_id INT, in_summarydata VARCHAR(255), 
                   in_detailsdata TEXT, in_fqan_id INT, in_vo_id INT, in_gatheredat_id INT, in_check_time INT, in_insert_time INT,
                   in_recalculationNeeded INT, in_maxAgeOfTupleInHours INT)
BEGIN

  DECLARE v_metricdetail_id INT; 
  IF in_service_id is not NULL and in_metric_id is not NULL THEN

    if in_detailsdata is not null then
      insert into metricdetails(detail)
      values (in_detailsdata);
 
      select last_insert_id() into v_metricdetail_id;
    end if;

    insert into metricdata(metricstatus_id, metricdetail_id, metric_id, service_id, fqan_id, vo_id, summarydata, gatheredat_id, check_time, insert_time)
    VALUES (in_metricstatus_id, v_metricdetail_id, in_metric_id, in_service_id, in_fqan_id, in_vo_id, in_summarydata, in_gatheredat_id, in_check_time, in_insert_time);

    call calculateStatusChange(in_service_id, in_metric_id, in_metricstatus_id, in_check_time, in_vo_id, in_recalculationNeeded, in_maxAgeOfTupleInHours);
    call calculateStatusChangeFqan(in_fqan_id, in_service_id, in_metric_id, in_metricstatus_id, in_check_time, in_vo_id, in_recalculationNeeded, in_maxAgeOfTupleInHours);

  END IF;

END //

DROP PROCEDURE IF EXISTS calculateStatusChange;

CREATE PROCEDURE calculateStatusChange(serviceId INT, metricId INT, metricstatusId INT, check_time INT, voId INT,
                                       recalculationNeeded INT, maxAgeOfTupleInHours INT)
BEGIN

  DECLARE v_profileId INT;
  DECLARE v_supportedVo INT;

  DECLARE done INT DEFAULT 0;
  DECLARE profileCurs CURSOR FOR select id from profile where lower(name) in (select lower(name) from supported_profiles);
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  call calculateStatusChangeMetric(metricId, serviceId, voId, metricstatusId, check_time);
  
  select id into v_supportedVo from vo where id = voId 
  and lower(voname) in (select name from supported_vos);


  IF (v_supportedVo is not null) or (voId is NULL) THEN
    OPEN profileCurs;
  
    REPEAT
  
      set done = 0;
      FETCH profileCurs INTO v_profileId;
 
      IF NOT done THEN
        IF checkIfCalculationNeeded(v_profileId, serviceId, metricId, voId, check_time) THEN
          IF (ifnull(getServiceStatus(v_profileId, serviceId, check_time), -1) != metricstatusId) THEN

            call calculateStatusChangeProfile(v_profileId, serviceId, metricId, metricstatusId, check_time, voId,
                                       recalculationNeeded, maxAgeOfTupleInHours);
            insert into tested_services (service_id, profile_id, last_insert_time)
            values(serviceId, v_profileId, unix_timestamp())
            on duplicate key update last_insert_time = unix_timestamp();
          END IF; 
        END IF;
        set done = 0; 
      END IF;
        
    UNTIL done END REPEAT;
   
  
    CLOSE profileCurs;
  END IF;   

END //


DROP PROCEDURE IF EXISTS calculateStatusChangeMetric;

CREATE PROCEDURE calculateStatusChangeMetric(metricId INT, serviceId INT, voId INT, metricstatusId INT, check_time INT)

BEGIN
  DECLARE num_row INTEGER;
  DECLARE old_metricstatus_id INTEGER;
  DECLARE old_timestamp INTEGER;

  SET num_row = 0;
 
  call findCurrentStatusMetricService(metricId, serviceId, voId, metricstatusId, check_time, old_metricstatus_id, old_timestamp, num_row); 

  IF num_row > 0 THEN
    IF ((old_timestamp != check_time) AND (metricstatusId != old_metricstatus_id)) THEN
      INSERT INTO statuschange_metric_service(metric_id, service_id, vo_id, metricstatus_id, timestamp, insert_time)
      VALUES(metricId, serviceId, voId, metricstatusId, check_time, UNIX_TIMESTAMP()); 
    ELSEIF (getMetricStatusWeightForId(metricstatusId) > getMetricStatusWeightForId(old_metricstatus_id)) THEN
       UPDATE statuschange_metric_service SET metricstatus_id = metricstatusId, insert_time = UNIX_TIMESTAMP()
         WHERE service_id = serviceId AND metric_id = metricId and timestamp = check_time
           AND ifnull(vo_id, -1) = ifnull(voId, -1);
    END IF;
  ELSE
    INSERT INTO statuschange_metric_service(metric_id, service_id, vo_id, metricstatus_id, timestamp, insert_time)
    VALUES(metricId, serviceId, voId, metricstatusId, check_time, UNIX_TIMESTAMP()); 
  END IF;

END //

DROP PROCEDURE IF EXISTS calculateStatusChangeProfile;

CREATE PROCEDURE calculateStatusChangeProfile(profileId INT, serviceId INT, metricId INT, metricstatusId INT, 
                     check_time INT, voId INT, recalculationNeeded INT, maxAgeOfTupleInHours INT)

BEGIN
  DECLARE old_metricstatus_id INTEGER; 
  DECLARE old_timestamp INTEGER; 
  DECLARE num_row INTEGER;
  DECLARE curr_servicestatus_id INTEGER;

  SET num_row = 0;

  call findCurrentStatus(profileId, serviceId, metricstatusId, check_time, voId, curr_servicestatus_id, old_metricstatus_id, old_timestamp, num_row); 

  IF num_row > 0 THEN
    IF (getMetricStatusWeightForId(curr_servicestatus_id) > getMetricStatusWeightForId(old_metricstatus_id)) THEN
      IF old_timestamp != check_time THEN
        INSERT INTO statuschange_service_profile(service_id, profile_id, metricstatus_id, detail, timestamp, insert_time)
        VALUES(serviceId, profileId, curr_servicestatus_id, NULL, check_time, UNIX_TIMESTAMP());
      ELSE
         UPDATE statuschange_service_profile SET metricstatus_id = curr_servicestatus_id, insert_time = UNIX_TIMESTAMP() WHERE service_id = serviceId AND profile_id = profileId and timestamp = check_time;
      END IF;

    ELSEIF ((getMetricStatusWeightForId(metricstatusId) < getMetricStatusWeightForId(old_metricstatus_id))
        AND (getMetricStatusWeightForId(curr_servicestatus_id) < getMetricStatusWeightForId(old_metricstatus_id))) THEN

      IF old_timestamp != check_time THEN
        INSERT INTO statuschange_service_profile(service_id, profile_id, metricstatus_id, detail, timestamp, insert_time)
        VALUES(serviceId, profileId, curr_servicestatus_id, NULL, check_time, UNIX_TIMESTAMP());
      ELSE
         UPDATE statuschange_service_profile SET metricstatus_id = curr_servicestatus_id, insert_time = UNIX_TIMESTAMP() WHERE service_id = serviceId AND profile_id = profileId and timestamp = check_time;
      END IF;

    END IF;
  ELSE 
    INSERT INTO statuschange_service_profile(service_id, profile_id, metricstatus_id, detail, timestamp, insert_time)
    VALUES(serviceId, profileId, metricstatusId, NULL, check_time, UNIX_TIMESTAMP()); 
  END IF;

END //

DROP FUNCTION IF EXISTS checkIfCalculationNeeded;

/*TODO: Needs to discuss if should look at void or only at table metrics_in_supported_services (Now this option will not work for profiles
like ROC_CRITICAL, ROC_OPERATORS)*/

CREATE FUNCTION checkIfCalculationNeeded(profileId INT, serviceId INT, metricId INT, voId INT, check_time INT)
RETURNS INT
BEGIN

  DECLARE v_counter INT;

  DECLARE metricCurs CURSOR FOR SELECT count(1) FROM profile_metric_map 
    where profile_id = profileId and (test_vo_id = voId or voId is null) 
       and metric_id = metricId and service_type_flavour_id =
        (select flavour_id from service where id = serviceId)
       and (del_time > from_unixtime(check_time) or del_time is null) 
       and profile_id in (select id from profile where lower(name) in (select lower(name) from supported_profiles));

  
  OPEN metricCurs;
  FETCH metricCurs INTO v_counter;
  CLOSE metricCurs;

  IF (v_counter > 0) THEN
    RETURN 1;
  ELSE 
    RETURN 0;
  END IF;

END //

/* TODO: This function will replace checkIfCalculationNeeded
   In the future will add checking if service defined in grouping, e.g. MPI, glexec */

DROP FUNCTION IF EXISTS checkIfMetricDefInProfile;

CREATE FUNCTION checkIfMetricDefInProfile(profileId INT, serviceId INT, metricId INT)
RETURNS INT
BEGIN

  DECLARE v_counter INT;

  DECLARE metricCurs CURSOR FOR SELECT count(1) FROM profile_metric_map 
    where profile_id = profileId and metric_id = metricId 
      and service_type_flavour_id =
        (select flavour_id from service where id = serviceId)
      and (del_time > from_unixtime(check_time) or del_time is null) 
      and profile_id in (select id from profile where lower(name) in (select lower(name) from supported_profiles));

  
  OPEN metricCurs;
  FETCH metricCurs INTO v_counter;
  CLOSE metricCurs;

  IF (v_counter > 0) THEN
    RETURN 1;
  ELSE 
    RETURN 0;
  END IF;

END //


DROP PROCEDURE IF EXISTS insertMissingMetrics;

CREATE PROCEDURE insertMissingMetrics(maxAgeInHours INT, ageInMinutes INT)
BEGIN

  DECLARE v_id INT;
  DECLARE v_metric_id INT;
  DECLARE v_service_id INT;
  DECLARE v_flavour_id INT;
  DECLARE v_vo_id INT;
  DECLARE v_fqan_id INT;

  DECLARE done INT DEFAULT 0;


  DECLARE oldMetricsCurs CURSOR FOR
    select m.id, m.metric_id, m.service_id, m.vo_id, m.fqan_id from metricdata_latest m,
      metrics_in_supported_services s
    where
         m.metric_id = s.metric_id
     and m.service_id = s.service_id
     and ifnull(m.vo_id, -1) = ifnull(s.vo_id, -1)
     and ifnull(m.fqan_id, -1) = ifnull(s.fqan_id, -1)
     and m.check_time <=  UNIX_TIMESTAMP(now() - interval 24 hour - interval 10 minute)
     and m.service_id in (select id from service where isdeleted = 'N')
     and m.metricstatus_id not in (select id from metricstatus where description = 'REMOVED');

  DECLARE newMissingMetricsCurs CURSOR FOR
    select metric_id, service_id, vo_id, fqan_id from
      (select distinct p.metric_id, p.service_id, p.vo_id, p.fqan_id, m.metric_id as m_metric_id from
         metrics_in_supported_services p left join metricdata_latest m on m.metric_id = p.metric_id
           and m.service_id = p.service_id
           and ifnull(m.vo_id, -1) = ifnull(p.vo_id, -1)
           and ifnull(m.fqan_id, -1) = ifnull(p.fqan_id, -1)
      ) a, service s where a.service_id = s.id and a.m_metric_id is null and s.isdeleted = 'N';


  /*Checking metricdata_spool not needed as spool was cleared by loadmetricdata*/

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  set autocommit = 0;

  OPEN oldMetricsCurs;

  REPEAT
    set done = 0;
    FETCH oldMetricsCurs INTO v_id, v_metric_id, v_service_id, v_vo_id, v_fqan_id;

    IF NOT done THEN

      insert into metricdata_spool(gatheredat_id, metricstatus_id, detailsdata, metric_id, service_id, vo_id, fqan_id, summarydata, check_time, insert_time)
        VALUES (NULL, (select id from metricstatus where description = 'MISSING'), NULL, v_metric_id, v_service_id, v_vo_id, v_fqan_id, NULL, unix_timestamp(now() - interval ageInMinutes minute), unix_timestamp());
     
      set done = 0; 
    END IF;
  UNTIL done END REPEAT;

  CLOSE oldMetricsCurs;

  commit;

  set done = 0;
  OPEN newMissingMetricsCurs;

  REPEAT
    set done = 0;
    FETCH newMissingMetricsCurs INTO v_metric_id, v_service_id, v_vo_id, v_fqan_id;
    
    IF NOT done THEN
      insert into metricdata_spool(gatheredat_id, metricstatus_id, detailsdata, metric_id, service_id, vo_id, fqan_id, summarydata, check_time, insert_time)
        VALUES (NULL, (select id from metricstatus where description = 'MISSING'), NULL, v_metric_id, v_service_id, v_vo_id, v_fqan_id, NULL, unix_timestamp(now() - interval ageInMinutes minute), unix_timestamp());

      set done = 0; 
    END IF;

  UNTIL done END REPEAT;
  CLOSE newMissingMetricsCurs;
  commit;

END //

DROP PROCEDURE IF EXISTS purgeMetricStore;

CREATE PROCEDURE purgeMetricStore(maxAgeInDays INT)
BEGIN
  
  DECLARE minInsertTime INT;

  set minInsertTime = UNIX_TIMESTAMP(now() - interval maxAgeInDays day); 

  update statuschange_service_profile
    set timestamp = minInsertTime, insert_time = UNIX_TIMESTAMP() 
    where id in (
      select maxId from (
        select max(id) as maxId, service_id, profile_id from statuschange_service_profile
          where timestamp <= minInsertTime
        group by service_id, profile_id
      ) a
    );


  DELETE FROM statuschange_service_profile
    WHERE timestamp < minInsertTime;


  DELETE FROM metricdata WHERE check_time < minInsertTime;

  /*Deleting data from metricdata_latest isn't neeeded since insertMissingMetrics will create "MISSING" metrics. 
    If we switch off insertMissingMetrics, we would have to delete entries from tested_services;
  */ 
  DELETE FROM metricdata_latest WHERE check_time < minInsertTime;

  DELETE FROM metricdata_rejected WHERE check_time < minInsertTime;

  DELETE FROM metricdetails where timestamp < minInsertTime;

  DELETE FROM statuschange_service_profile WHERE service_id not in
      (select distinct service_id from metricdata_latest)
    and service_id not in (select distinct service_id from metrics_in_supported_services);

  DELETE FROM statuschange_serv_profile_fqan WHERE service_id not in
      (select distinct service_id from metricdata_latest)
    and service_id not in (select distinct service_id from metrics_in_supported_services);

  DELETE FROM statuschange_metric_service WHERE service_id not in
      (select distinct service_id from metricdata_latest)
    and service_id not in (select distinct service_id from metrics_in_supported_services);

  DELETE FROM statuschange_metric_serv_fqan WHERE service_id not in
      (select distinct service_id from metricdata_latest)
    and service_id not in (select distinct service_id from metrics_in_supported_services);

  DELETE FROM tested_services WHERE service_id not in
      (select distinct service_id from metricdata_latest)
    and service_id not in (select distinct service_id from metrics_in_supported_services);

  DELETE FROM tested_services_fqan WHERE service_id not in
      (select distinct service_id from metricdata_latest)
    and service_id not in (select distinct service_id from metrics_in_supported_services);


  COMMIT;

END //

DROP FUNCTION IF EXISTS getOrAddGatheredAt;

CREATE FUNCTION getOrAddGatheredAt(in_gatheredat VARCHAR(255)) RETURNS INT
BEGIN
  DECLARE v_id INT;

  select id into v_id from gatheredat where lower(name) = lower(in_gatheredat);

  IF v_id is null THEN
    insert into gatheredat(name)
      values(in_gatheredat);
    select id into v_id from gatheredat where lower(name) = lower(in_gatheredat);
  END IF;
   
  RETURN v_id;

END //

DROP FUNCTION IF EXISTS getOrAddFqan;

CREATE FUNCTION getOrAddFqan(in_name VARCHAR(255)) RETURNS INT
BEGIN
  DECLARE v_id INT;
  set v_id = NULL;

  IF in_name is not NULL THEN
    select id into v_id from fqan where lower(name) = lower(in_name);
    
    IF v_id is null THEN

      insert into fqan(name)
        values(in_name);
      select id into v_id from fqan where lower(name) = lower(in_name);
    END IF;
  END IF; 

  RETURN v_id;
END //


DROP PROCEDURE IF EXISTS addSupportedVos;

CREATE PROCEDURE addSupportedVos(in_VoList VARCHAR(255))
BEGIN
  DECLARE intFoundPos INT;
  DECLARE strElement VARCHAR(255);
  DECLARE v_VoName VARCHAR(255);

  DECLARE done INT DEFAULT 0;
  DECLARE voCurs CURSOR FOR select name from tmpVoList;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  DROP TEMPORARY TABLE IF EXISTS tmpVoList;
  CREATE TEMPORARY TABLE tmpVoList ( `name` VARCHAR(50) NOT NULL DEFAULT '' ) ENGINE = MEMORY;

  SET intFoundPos = INSTR(in_VoList,',');

  WHILE intFoundPos <> 0 do
    SET strElement = SUBSTRING(in_VoList, 1, intFoundPos-1);
    SET in_VoList = REPLACE(in_VoList, CONCAT(strElement,','), '');
    INSERT INTO tmpVoList (`name`) VALUES ( strElement);
    SET intFoundPos = INSTR(in_VoList,',');
  END WHILE;

  IF in_VoList <> '' THEN
    INSERT INTO tmpVoList (`name`) VALUES (in_VoList);
  END IF; 

  delete from supported_vos;

  OPEN voCurs;

  REPEAT 
    set done = 0;
    FETCH voCurs INTO v_VoName;

    IF NOT done THEN
      insert into supported_vos(name, insert_time) values(lower(v_VoName), unix_timestamp())
        on duplicate key update name = v_VoName;

      set done = 0;
    END IF;

  UNTIL done END REPEAT;

  CLOSE voCurs;

  COMMIT;

END //

DROP PROCEDURE IF EXISTS addSupportedProfiles;

CREATE PROCEDURE addSupportedProfiles(in_ProfileList VARCHAR(255))
BEGIN
  DECLARE intFoundPos INT;
  DECLARE strElement VARCHAR(255);
  DECLARE v_ProfileName VARCHAR(255);

  DECLARE done INT DEFAULT 0;
  DECLARE profileCurs CURSOR FOR select name from tmpProfileList;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  DROP TEMPORARY TABLE IF EXISTS tmpProfileList;
  CREATE TEMPORARY TABLE tmpProfileList ( `name` VARCHAR(50) NOT NULL DEFAULT '' ) ENGINE = MEMORY;

  SET intFoundPos = INSTR(in_ProfileList,',');

  WHILE intFoundPos <> 0 do
    SET strElement = SUBSTRING(in_ProfileList, 1, intFoundPos-1);
    SET in_ProfileList = REPLACE(in_ProfileList, CONCAT(strElement,','), '');
    INSERT INTO tmpProfileList (`name`) VALUES ( strElement);
    SET intFoundPos = INSTR(in_ProfileList,',');
  END WHILE;

  IF in_ProfileList <> '' THEN
    INSERT INTO tmpProfileList (`name`) VALUES (in_ProfileList);
  END IF; 

  truncate table supported_profiles;

  OPEN profileCurs;

  REPEAT 
  
    set done = 0;
    FETCH profileCurs INTO v_ProfileName;
    SET v_ProfileName = lower(v_ProfileName);

    IF NOT done THEN
      insert into supported_profiles (name, insert_time)
      values(v_ProfileName, unix_timestamp())
       on duplicate key update name = v_ProfileName;

      set done = 0;
    END IF;

  UNTIL done END REPEAT;

  CLOSE profileCurs;

  COMMIT;

END //

DROP PROCEDURE IF EXISTS _addFqanToSupportedServicesForProfile;

CREATE PROCEDURE _addFqanToSupportedServicesForProfile(v_service_id INT, v_fqan_id INT, v_profile_id INT)

BEGIN
  DECLARE v_id INT;
  DECLARE done INT DEFAULT 0;
  DECLARE fqan_curs CURSOR FOR select id from fqans_in_supported_services 
    where service_id = v_service_id 
      and fqan_id = v_fqan_id and profile_id = v_profile_id;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  OPEN fqan_curs;
  set v_id = 0;

  FETCH fqan_curs INTO v_id;
    IF v_id != 0 THEN
      update fqans_in_supported_services set timestamp = unix_timestamp(), updated = 1 where id = v_id;
    ELSE
      insert into fqans_in_supported_services (service_id, fqan_id, profile_id, timestamp, updated)
      values(v_service_id, v_fqan_id, v_profile_id, unix_timestamp(), 1);
    END IF;

  CLOSE fqan_curs;

END;

DROP PROCEDURE IF EXISTS addFqanToSupportedServices;

CREATE PROCEDURE addFqanToSupportedServices(in_service_type_flavour VARCHAR(255), in_hostname VARCHAR(255), 
                 in_fqan VARCHAR(255), in_profile VARCHAR(255))
BEGIN
  DECLARE v_fqan_id INT;
  DECLARE v_serviceflavour_id INT;
  DECLARE v_service_id INT;
  DECLARE v_profile_id INT;
  DECLARE v_rejection_reason VARCHAR(255);

  DECLARE done INT DEFAULT 0;
  DECLARE profile_curs CURSOR FOR select id from profile where lower(name) in (select lower(name) from supported_profiles) 
    and lower(name) like lower(concat(in_profile, '%')); 

  /*TODO: Check if service_id is supported by given profile should be added when it will be supported by MDDB and ATP*/

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  set in_service_type_flavour = IF(in_service_type_flavour='sBDII', 'Site-BDII', in_service_type_flavour); 

  select id into v_serviceflavour_id from service_type_flavour where lower(flavourname) = lower(in_service_type_flavour);
  select id into v_service_id from service where lower(hostname) = lower(in_hostname) and flavour_id = v_serviceflavour_id;
  select getOrAddFqan(in_fqan) into v_fqan_id;

  OPEN profile_curs;


  REPEAT
    set done = 0;
    FETCH profile_curs INTO v_profile_id;
  
    IF NOT done THEN

      IF (v_service_id is not NULL) AND (v_profile_id is not NULL) THEN
        call _addFqanToSupportedServicesForProfile(v_service_id, v_fqan_id, v_profile_id);
      ELSE
        IF v_service_id is NULL THEN
          set v_rejection_reason = 'service_id is NULL';
        END IF; 
        IF v_profile_id is NULL THEN
          set v_rejection_reason = CONCAT_WS(', ', v_rejection_reason,  'profile_id is NULL');
        END IF;
        insert into fqans_in_supp_serv_rejected (service_type_flavour, hostname, fqan, profile, timestamp, reason)
        values(in_service_type_flavour, in_hostname, in_fqan, in_profile, unix_timestamp(), v_rejection_reason);
        set done = 0; 
      END IF;

    END IF;

  UNTIL done END REPEAT;
  commit;

  CLOSE profile_curs;

END //

DROP PROCEDURE IF EXISTS addMetricToSupportedServices;

CREATE PROCEDURE addMetricToSupportedServices(in_service_type_flavour VARCHAR(255), in_hostname VARCHAR(255), in_vo VARCHAR(255),
                 in_fqan VARCHAR(255), in_profile VARCHAR(255), in_metricname VARCHAR(255), in_frequency INT)
BEGIN
  DECLARE v_metric_id INT;
  DECLARE v_fqan_id INT;
  DECLARE v_vo_id INT;
  DECLARE v_serviceflavour_id INT;
  DECLARE v_service_id INT;
  DECLARE v_id INT;
  DECLARE v_profile_id INT;
  DECLARE v_rejection_reason VARCHAR(255);

  DECLARE done INT DEFAULT 0;
  DECLARE metricCurs CURSOR FOR select id from metrics_in_supported_services 
    where service_id = v_service_id 
    and metric_id = v_metric_id and ifnull(vo_id, -1) = ifnull(v_vo_id, -1) 
    and ifnull(fqan_id, -1) = ifnull(v_fqan_id, -1) and profile_id = v_profile_id;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  set in_service_type_flavour = IF(in_service_type_flavour='sBDII', 'Site-BDII', in_service_type_flavour); 

  select id into v_metric_id from metric where lower(name) = lower(in_metricname) 
    and version = (select max(version) from metric where lower(name) = lower(in_metricname));
  select id into v_vo_id from vo where lower(voname) = lower(in_vo);
  select id into v_serviceflavour_id from service_type_flavour where lower(flavourname) = lower(in_service_type_flavour);
  select id into v_service_id from service where lower(hostname) = lower(in_hostname) and flavour_id = v_serviceflavour_id;
  select id into v_profile_id from profile where lower(name) = lower(in_profile); 
  select getOrAddFqan(in_fqan) into v_fqan_id;

  OPEN metricCurs;

  set v_id = 0;

  FETCH metricCurs INTO v_id;
  IF (v_metric_id is not NULL) AND (v_service_id is not NULL) AND (v_profile_id is not NULL) THEN
    IF v_id != 0 THEN
      update metrics_in_supported_services set timestamp = unix_timestamp(), frequency = in_frequency, updated = 1 where id = v_id;
    ELSE
      insert into metrics_in_supported_services (service_id, metric_id, vo_id, fqan_id, profile_id, frequency, timestamp, updated)
      values(v_service_id, v_metric_id, v_vo_id, v_fqan_id, v_profile_id, in_frequency, unix_timestamp(), 1);
    END IF;
  ELSE
    IF v_metric_id is NULL THEN
      set v_rejection_reason = 'metric_id is NULL';
    END IF; 
    IF v_service_id is NULL THEN
      set v_rejection_reason = CONCAT_WS(', ', v_rejection_reason,  'service_id is NULL');
    END IF; 
    IF v_profile_id is NULL THEN
      set v_rejection_reason = CONCAT_WS(', ', v_rejection_reason,  'profile_id is NULL');
    END IF;
    

    insert into metrics_in_supp_serv_rejected (service_type_flavour, hostname, metric_name, vo, fqan, profile, frequency, timestamp, reason)
    values(in_service_type_flavour, in_hostname, in_metricname, in_vo, in_fqan, in_profile, in_frequency, unix_timestamp(), v_rejection_reason);
  END IF;
  commit;

  CLOSE metricCurs;
END //

DROP FUNCTION IF EXISTS getMetricStatusWeightForId; 

CREATE FUNCTION getMetricStatusWeightForId (in_id INT) RETURNS INT 

BEGIN
  DECLARE v_weight INT;
   
  select weight into v_weight from metricstatus where id = in_id;    
  RETURN v_weight;

END // 

DROP FUNCTION IF EXISTS getServiceStatus;

CREATE FUNCTION getServiceStatus(
  profileId INT,
  serviceId INT,
  checkTime INT) RETURNS INT

BEGIN

  DECLARE statusId INT;

  select metricstatus_id into statusId from statuschange_service_profile 
    where timestamp = (select max(timestamp) from statuschange_service_profile
                       where service_id = serviceId and profile_id = profileId
                             and timestamp <= checkTime)
    and service_id = serviceId and profile_id = profileId;

  RETURN statusId;

END //

DROP PROCEDURE IF EXISTS findCurrentStatus;

CREATE PROCEDURE findCurrentStatus (profileId INT, serviceId INT, metricstatusId INT, check_time INT,
  voId INT,
  OUT curr_servicestatus_id INT,
  OUT old_metricstatus_id INT,
  OUT old_timestamp INT,
  OUT num_row INT
  )

BEGIN
  DECLARE curr_weight INT;  
 
  select count(*) INTO num_row
  from statuschange_service_profile 
  where service_id = serviceId and profile_id = profileId
    AND timestamp <= check_time;

  IF num_row > 0 THEN
    SELECT metricstatus_id, timestamp INTO old_metricstatus_id, old_timestamp
    FROM statuschange_service_profile 
    WHERE service_id = serviceId AND profile_id = profileId AND timestamp =
      (SELECT max(timestamp) FROM statuschange_service_profile
      WHERE service_id = serviceId AND profile_id = profileId and timestamp <= check_time);

    IF (getMetricStatusWeightForId(metricstatusId) != getMetricStatusWeightForId(old_metricstatus_id)) THEN

      select max(s.weight) INTO curr_weight from metricdata_latest l, metricstatus s where l.id in (
        select lastId from (
          select max(id) as lastId, metric_id from metricdata_latest where metric_id in (
              select m.metric_id from profile_metric_map m
                 where m.profile_id = profileId
                  and (m.test_vo_id = voId or voId is null)
                  and m.service_type_flavour_id = (select flavour_id from service l where l.id = serviceId)
                  and (m.del_time > from_unixtime(check_time) or m.del_time is null)
              )
            and service_id = serviceId 
            and (ifnull(vo_id, -1) = ifnull(voId, -1) or vo_id is null)
            and (vo_id is null or vo_id in ( 
              SELECT m.test_vo_id FROM profile_metric_map m
                where m.profile_id = profileId 
                  and (test_vo_id = voId or voId is null)
                  and m.service_type_flavour_id = (select flavour_id from service l where l.id = serviceId)
                  and (m.del_time > from_unixtime(check_time) or m.del_time is null)
              ))                        
            group by metric_id) a
        ) and l.metricstatus_id = s.id;
       
      select id into curr_servicestatus_id from metricstatus where weight = curr_weight;   

    END IF;
  
  END IF;

END //

DROP PROCEDURE IF EXISTS findCurrentStatusMetricService;

CREATE PROCEDURE findCurrentStatusMetricService (metricId INT, serviceId INT, voId INT, metricstatusId INT, check_time INT,
  OUT old_metricstatus_id INT,
  OUT old_timestamp INT,
  OUT num_row INT)
BEGIN
  
  select count(*) INTO num_row
  from statuschange_metric_service 
  where ifnull(vo_id, -1) = ifnull(voId, -1)
    AND metric_id = metricId 
    AND service_id = serviceId
    AND timestamp <= check_time;

  IF num_row > 0 THEN

    SELECT metricstatus_id, timestamp INTO old_metricstatus_id, old_timestamp
    FROM statuschange_metric_service 
    WHERE ifnull(vo_id, -1) = ifnull(voId, -1)
      AND metric_id = metricId
      AND service_id = serviceId 
      AND timestamp =
        (SELECT max(timestamp) FROM statuschange_metric_service
          WHERE ifnull(vo_id, -1) = ifnull(voId, -1)
            AND metric_id = metricId
            AND service_id = serviceId 
            AND timestamp <= check_time);

  END IF;

END //

DROP PROCEDURE IF EXISTS loadmetricdata;

CREATE PROCEDURE loadmetricdata(maxAgeInHours INT, ageInMinutes INT)
                  
  BEGIN

  DECLARE v_minCheckTime INT;  
  DECLARE done INT DEFAULT 0;
 
  DECLARE v_id, v_fqan_id, v_gatheredat_id, v_metricstatus_id, v_check_time, v_insert_time,  v_metric_id, v_service_id, v_vo_id INT;
  DECLARE v_summarydata VARCHAR(256);
  DECLARE v_detailsdata TEXT;


  DECLARE metricResultsCurs CURSOR FOR select id, gatheredat_id, metricstatus_id, detailsdata, metric_id, service_id,
    fqan_id, vo_id, summarydata, check_time, insert_time from metricdata_spool
      where check_time < unix_timestamp(now() - interval ageInMinutes minute)
      order by check_time; 

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  set autocommit = 0;

  OPEN metricResultsCurs;
 
  REPEAT
    set done = 0;
    FETCH metricResultsCurs INTO v_id, v_gatheredat_id, v_metricstatus_id, v_detailsdata, v_metric_id, v_service_id,
        v_fqan_id, v_vo_id, v_summarydata, v_check_time, v_insert_time;
     

    IF NOT done THEN
      call insertRowToMetricData(v_service_id, v_metric_id, v_metricstatus_id, v_summarydata, v_detailsdata,
             v_fqan_id, v_vo_id, v_gatheredat_id, v_check_time, v_insert_time, false, maxAgeInHours);

      delete from metricdata_spool where id = v_id;
      commit;

      set done = 0;
    END IF;

  UNTIL done END REPEAT;

  CLOSE metricResultsCurs;

  call insertMissingMetrics(maxAgeInHours, ageInMinutes);
 
  commit;
END //

/*FQAN - status change*/

DROP PROCEDURE IF EXISTS calculateStatusChangeFqan;

CREATE PROCEDURE calculateStatusChangeFqan(fqanId INT, serviceId INT, metricId INT, metricstatusId INT, check_time INT, voId INT,
                                       recalculationNeeded INT, maxAgeOfTupleInHours INT)
BEGIN
  DECLARE v_fqanId INT;
  DECLARE v_profileId INT;
  DECLARE done INT DEFAULT 0;
  DECLARE fqanCurs CURSOR FOR select distinct profile_id, fqan_id from fqans_in_supported_services where service_id = serviceId and ((fqan_id = fqanId) or (fqanId is null));
  DECLARE fqanCursHist CURSOR FOR select distinct profile_id, fqan_id from fqans_in_supp_serv_history where service_id = serviceId and ((fqan_id = fqanId) or (fqanId is null)) and timestamp >= check_time and operation = 'delete'; 
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  call calculateStatusChangeMetricFqan(metricId, serviceId, fqanId, voId, metricstatusId, check_time);

  IF metricstatusId = 6 THEN
    OPEN fqanCursHist;
  ELSE
    OPEN fqanCurs;
  END IF;


  REPEAT

    set done = 0;
    IF metricstatusId = 6 THEN
      FETCH fqanCursHist INTO v_profileId, v_fqanId;
    ELSE
      FETCH fqanCurs INTO v_profileId, v_fqanId;
    END IF;
    
    IF NOT done THEN
      IF checkIfCalculationNeeded(v_profileId, serviceId, metricId, voId, check_time) THEN
        IF (ifnull(getServiceStatusFqan(v_fqanId, v_profileId, serviceId, check_time), -1) != metricstatusId) THEN

          call calculateStatusChangeProfileFqan(v_profileId, serviceId, v_fqanId, metricId, metricstatusId, check_time, voId,
                                     recalculationNeeded, maxAgeOfTupleInHours);


          insert into tested_services_fqan (service_id, profile_id, fqan_id, last_insert_time)
          values(serviceId, v_profileId, v_fqanId, unix_timestamp())
          on duplicate key update last_insert_time = unix_timestamp();
        END IF; 
      END IF;

      set done = 0;
    END IF;
      
  UNTIL done END REPEAT;
 
  IF metricstatusId = 6 THEN
    CLOSE fqanCursHist;
  ELSE
    CLOSE fqanCurs;
  END IF;

END //

DROP PROCEDURE IF EXISTS calculateStatusChangeMetricFqan;

CREATE PROCEDURE calculateStatusChangeMetricFqan(metricId INT, serviceId INT, fqanId INT, voId INT, metricstatusId INT, check_time INT)

BEGIN
  DECLARE num_row INTEGER;
  DECLARE old_metricstatus_id INTEGER;
  DECLARE old_timestamp INTEGER;

  SET num_row = 0;

  call findCurrentStatusMetricServiceFqan(metricId, serviceId, fqanId, voId, metricstatusId, check_time, old_metricstatus_id, old_timestamp, num_row); 

  IF num_row > 0 THEN
    IF ((old_timestamp != check_time) AND (metricstatusId != old_metricstatus_id)) THEN
      INSERT INTO statuschange_metric_serv_fqan(metric_id, service_id, fqan_id, vo_id, metricstatus_id, timestamp, insert_time)
        VALUES(metricId, serviceId, fqanId, voId, metricstatusId, check_time, UNIX_TIMESTAMP()); 
    ELSEIF (getMetricStatusWeightForId(metricstatusId) > getMetricStatusWeightForId(old_metricstatus_id)) THEN
       UPDATE statuschange_metric_serv_fqan SET metricstatus_id = metricstatusId, insert_time = UNIX_TIMESTAMP()
         WHERE service_id = serviceId AND metric_id = metricId and timestamp = check_time
           AND ifnull(fqan_id, -1) = ifnull(fqanId, -1);
    END IF;
  ELSE
    INSERT INTO statuschange_metric_serv_fqan(metric_id, service_id, fqan_id, vo_id, metricstatus_id, timestamp, insert_time)
    VALUES(metricId, serviceId, fqanId, voId, metricstatusId, check_time, UNIX_TIMESTAMP()); 
  END IF;

END //

DROP PROCEDURE IF EXISTS calculateStatusChangeProfileFqan;

CREATE PROCEDURE calculateStatusChangeProfileFqan(profileId INT, serviceId INT, fqanId INT, metricId INT, metricstatusId INT, 
                     check_time INT, voId INT, recalculationNeeded INT, maxAgeOfTupleInHours INT)

BEGIN
  DECLARE old_metricstatus_id INTEGER; 
  DECLARE old_timestamp INTEGER; 
  DECLARE num_row INTEGER;
  DECLARE curr_servicestatus_id INTEGER;

  SET num_row = 0;

--  IF (recalculationNeeded) THEN
--    call findCurrentStatusRecalculation(profileId, serviceId, metricstatusId, check_time, voId, maxAgeOfTupleInHours, curr_servicestatus_id, old_metricstatus_id, old_timestamp, num_row);
--  ELSE
  call findCurrentStatusFqan(profileId, serviceId, fqanId, metricstatusId, check_time, voId, curr_servicestatus_id, old_metricstatus_id, old_timestamp, num_row); 
--  END IF;

  IF num_row > 0 THEN

    IF (getMetricStatusWeightForId(curr_servicestatus_id) > getMetricStatusWeightForId(old_metricstatus_id)) THEN
      IF old_timestamp != check_time THEN
        INSERT INTO statuschange_serv_profile_fqan(fqan_id, service_id, profile_id, metricstatus_id, timestamp, insert_time)
        VALUES(fqanId, serviceId, profileId, curr_servicestatus_id, check_time, UNIX_TIMESTAMP());
      ELSE
         UPDATE statuschange_serv_profile_fqan SET metricstatus_id = curr_servicestatus_id, insert_time = UNIX_TIMESTAMP() WHERE service_id = serviceId AND profile_id = profileId and timestamp = check_time and fqan_id = fqanId;
      END IF;

    ELSEIF ((getMetricStatusWeightForId(metricstatusId) < getMetricStatusWeightForId(old_metricstatus_id)) 
        AND (getMetricStatusWeightForId(curr_servicestatus_id) < getMetricStatusWeightForId(old_metricstatus_id))) THEN

      IF old_timestamp != check_time THEN
        INSERT INTO statuschange_serv_profile_fqan(fqan_id, service_id, profile_id, metricstatus_id, timestamp, insert_time)
        VALUES(fqanId, serviceId, profileId, curr_servicestatus_id, check_time, UNIX_TIMESTAMP());
      ELSE
         UPDATE statuschange_serv_profile_fqan SET metricstatus_id = curr_servicestatus_id, insert_time = UNIX_TIMESTAMP() WHERE service_id = serviceId AND profile_id = profileId and timestamp = check_time and fqan_id = fqanId;
      END IF;

    END IF;
  ELSE 
    INSERT INTO statuschange_serv_profile_fqan(fqan_id, service_id, profile_id, metricstatus_id, timestamp, insert_time)
    VALUES(fqanId, serviceId, profileId, metricstatusId, check_time, UNIX_TIMESTAMP()); 
  END IF;

END //

DROP PROCEDURE IF EXISTS findCurrentStatusFqan;

CREATE PROCEDURE findCurrentStatusFqan (profileId INT, serviceId INT, fqanId INT, metricstatusId INT, check_time INT,
  voId INT,
  OUT curr_servicestatus_id INT,
  OUT old_metricstatus_id INT,
  OUT old_timestamp INT,
  OUT num_row INT
  )

BEGIN
  
  DECLARE curr_weight INT;

  select count(*) INTO num_row
  from statuschange_serv_profile_fqan 
  where service_id = serviceId and profile_id = profileId and fqan_id = fqanId
    and timestamp <= check_time;

  IF num_row > 0 THEN
    SELECT metricstatus_id, timestamp INTO old_metricstatus_id, old_timestamp
    FROM statuschange_serv_profile_fqan 
    WHERE service_id = serviceId AND profile_id = profileId AND fqan_id = fqanId
      AND timestamp =
        (SELECT max(timestamp) FROM statuschange_serv_profile_fqan
        WHERE service_id = serviceId AND profile_id = profileId 
          AND fqan_id = fqanId and timestamp <= check_time);

    IF (getMetricStatusWeightForId(metricstatusId) < getMetricStatusWeightForId(old_metricstatus_id)) THEN
        select max(weight) INTO curr_weight from metricdata_latest, metricstatus s where metric_id in (
          SELECT m.metric_id FROM profile_metric_map m
            where m.profile_id = profileId 
              and (test_vo_id = voId or voId is null)
              and m.service_type_flavour_id = (select flavour_id from service l where l.id = serviceId)
              and (m.del_time > from_unixtime(check_time) or m.del_time is null)
          ) 
          and service_id = serviceId and (ifnull(fqan_id, -1) = ifnull(fqanId, -1) or fqan_id is NULL)
          and (vo_id is null or vo_id in ( 
              SELECT m.test_vo_id FROM profile_metric_map m
                where m.profile_id = profileId 
                  and (test_vo_id = voId or voId is null)
                  and m.service_type_flavour_id = (select flavour_id from service l where l.id = serviceId)
                  and (m.del_time > from_unixtime(check_time) or m.del_time is null)
           ))
          and (ifnull(vo_id, -1) = ifnull(voId, -1) or vo_id is NULL)
          and metricstatus_id = s.id;

      select id into curr_servicestatus_id from metricstatus where weight = curr_weight;
    END IF;
  
  END IF;

END //

DROP PROCEDURE IF EXISTS findCurrentStatusMetricServiceFqan;

CREATE PROCEDURE findCurrentStatusMetricServiceFqan (metricId INT, serviceId INT, fqanId INT, voId INT, metricstatusId INT, check_time INT,
  OUT old_metricstatus_id INT,
  OUT old_timestamp INT,
  OUT num_row INT)
BEGIN
  
  select count(*) INTO num_row
  from statuschange_metric_serv_fqan 
  where ifnull(fqan_id, -1) = ifnull(fqanId, -1)
    AND metric_id = metricId 
    AND service_id = serviceId
    AND timestamp <= check_time;

  IF num_row > 0 THEN

    SELECT metricstatus_id, timestamp INTO old_metricstatus_id, old_timestamp
    FROM statuschange_metric_serv_fqan 
    WHERE ifnull(fqan_id, -1) = ifnull(fqanId, -1)
      AND metric_id = metricId
      AND service_id = serviceId 
      AND timestamp =
        (SELECT max(timestamp) FROM statuschange_metric_serv_fqan
          WHERE ifnull(fqan_id, -1) = ifnull(fqanId, -1)
            AND metric_id = metricId
            AND service_id = serviceId 
            AND timestamp <= check_time);

  END IF;

END //

DROP FUNCTION IF EXISTS getServiceStatusFqan;

CREATE FUNCTION getServiceStatusFqan(
  fqanId INT,
  profileId INT,
  serviceId INT,
  checkTime INT) RETURNS INT

BEGIN

  DECLARE statusId INT;

  select metricstatus_id into statusId from statuschange_serv_profile_fqan 
    where timestamp = (select max(timestamp) from statuschange_serv_profile_fqan
                       where service_id = serviceId and profile_id = profileId
                             and fqan_id = fqanId
                             and timestamp <= checkTime)
    and service_id = serviceId and profile_id = profileId and fqan_id = fqanId;

  RETURN statusId;

END //

/*END: FQAN - status change*/

DROP PROCEDURE IF EXISTS markRemovedMetrics;

CREATE PROCEDURE markRemovedMetrics()

BEGIN
  DECLARE v_metric_id INT;
  DECLARE v_service_id INT;
  DECLARE v_vo_id INT;
  DECLARE v_fqan_id INT;
  DECLARE done INT DEFAULT 0;

  DECLARE removedMetricsCurs CURSOR FOR
    select m_metric_id, service_id, vo_id, fqan_id from (
      select distinct p.metric_id as p_metric_id, m.service_id, m.vo_id, m.fqan_id, m.metric_id as m_metric_id, m.metricstatus_id from
             metricdata_latest m  left join metrics_in_supported_services p on m.metric_id = p.metric_id
               and m.service_id = p.service_id
               and ifnull(m.vo_id, -1) = ifnull(p.vo_id, -1)
               and ifnull(m.fqan_id, -1) = ifnull(p.fqan_id, -1)           
      ) a where a.p_metric_id is NULL and a.metricstatus_id != 6;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  OPEN removedMetricsCurs;

  REPEAT
    set done = 0;
    FETCH removedMetricsCurs INTO v_metric_id, v_service_id, v_vo_id, v_fqan_id;

    IF NOT done THEN
      insert into metricdata_spool(gatheredat_id, metricstatus_id, detailsdata, metric_id, service_id, vo_id, fqan_id, summarydata, check_time, insert_time)
        VALUES (NULL, 6, NULL, v_metric_id, v_service_id, v_vo_id, v_fqan_id, NULL, unix_timestamp(), unix_timestamp());

      set done = 0; 
    END IF;
  UNTIL done END REPEAT;


  CLOSE removedMetricsCurs;
  
END //

DELIMITER ;
