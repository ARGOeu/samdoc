delimiter //

use 'mrs';

DROP PROCEDURE IF EXISTS calculateStatusChange;

CREATE PROCEDURE calculateStatusChange(serviceId INT,  poem_metricId INT, metricstatusId INT, check_time INT, voId INT,
                                       recalculationNeeded INT, maxAgeOfTupleInHours INT)
BEGIN

  DECLARE profile_name VARCHAR(256);
  DECLARE poem_profileId INT;
  DECLARE v_supportedVo INT;

  DECLARE done INT DEFAULT 0;
 
  DECLARE poemProfileCurs CURSOR FOR
    select a.profile_id as id, c.name
        from poem_sync_p_m_instances a, poem_sync_metricinstance b, poem_sync_profile c where
          a.profile_id=c.id and c.isdeleted='N' and
          a.metricinstance_id=b.id and
          (b.atp_vo_id = voId or b.atp_vo_id is null or voId is null) and
           b.atp_service_type_flavour_id = (select flavour_id from service l where l.id = serviceId) and
          b.metric_id = poem_metricId /*and
          (b.fqan_id is null or b.fqan_id in (select id from poem_sync_fqan where fqan='/Role/lcgadm'))*/;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  call calculateStatusChangeMetric(poem_metricId, serviceId, voId, metricstatusId, check_time);

  select id into v_supportedVo from vo where id = voId
  and lower(voname) in (select name from supported_vos);

  IF (v_supportedVo is not null) or (voId is NULL) THEN
    OPEN poemProfileCurs;

    REPEAT

      set done = 0;
      FETCH poemProfileCurs INTO poem_profileId, profile_name;
        
      IF NOT done THEN
        IF checkIfCalculationNeeded(poem_profileId, serviceId, poem_metricId, voId, check_time) THEN
          IF (ifnull(getServiceStatus(poem_profileId, serviceId, check_time), -1) != metricstatusId) THEN

            call calculateStatusChangeProfile(poem_profileId, serviceId, poem_metricId, metricstatusId, check_time, voId,
                                       recalculationNeeded, maxAgeOfTupleInHours);
          END IF;
        END IF;
        set done = 0;
      END IF;

    UNTIL done END REPEAT;

    CLOSE poemProfileCurs;
  END IF;   

END //

delimiter ;
