delimiter //

use 'mrs';

DROP PROCEDURE IF EXISTS insertRowToMetricData;

CREATE PROCEDURE insertRowToMetricData(in_service_id INT, in_poem_metric_id INT, in_metricstatus_id INT, in_summarydata VARCHAR(255), 
                   in_detailsdata TEXT, in_fqan_id INT, in_vo_id INT, in_gatheredat_id INT, in_check_time INT, in_insert_time INT,
                   in_recalculationNeeded INT, in_maxAgeOfTupleInHours INT)
BEGIN

  DECLARE v_metricdetail_id INT; 
  DECLARE v_status_id INT;
  DECLARE v_num_row INT;

  IF in_service_id is not NULL and in_poem_metric_id is not NULL THEN

    set v_status_id = 0;
    set v_num_row = 0;

    if in_detailsdata is not null then
      insert into metricdetails(detail, timestamp)
      values (in_detailsdata,  unix_timestamp());
 
      select last_insert_id() into v_metricdetail_id;
    end if;
    
    select id into v_status_id from metricstatus where description = 'REMOVED';
    
    select count(*) into v_num_row
      from mrs_bootstrapper
      where metric_id = in_poem_metric_id
      and service_id = in_service_id
      and (ifnull(vo_id, -1) = ifnull(in_vo_id, -1) or vo_id is null)
      and (ifnull(fqan_id, -1) = ifnull(in_fqan_id, -1) or fqan_id is null);

    /*insert into metricdata(metricstatus_id, metricdetail_id, poem_sync_metric_id, service_id, fqan_id, vo_id, summarydata, gatheredat_id, check_time, insert_time)
    VALUES (in_metricstatus_id, v_metricdetail_id, in_poem_metric_id, in_service_id, in_fqan_id, in_vo_id, in_summarydata, in_gatheredat_id, in_check_time, in_insert_time);

    call calculateStatusChange(in_service_id, in_poem_metric_id, in_metricstatus_id, in_check_time, in_vo_id, in_recalculationNeeded, in_maxAgeOfTupleInHours);
*/

    if v_num_row > 0 or in_metricstatus_id = v_status_id then
        set v_num_row = 0;
        select count(*) INTO v_num_row from metricdata 
          where ifnull(vo_id, -1) = ifnull(in_vo_id, -1)
            and metricstatus_id = in_metricstatus_id
            and service_id = in_service_id
            and check_time = in_check_time
            and poem_sync_metric_id = in_poem_metric_id;
    
          IF v_num_row =  0 THEN
            insert into metricdata(metricstatus_id, metricdetail_id, poem_sync_metric_id, service_id, fqan_id, vo_id, summarydata, gatheredat_id, check_time, insert_time)
                VALUES (in_metricstatus_id, v_metricdetail_id, in_poem_metric_id, in_service_id, in_fqan_id, in_vo_id, in_summarydata, in_gatheredat_id, in_check_time, in_insert_time);
    
            call calculateStatusChange(in_service_id, in_poem_metric_id, in_metricstatus_id, in_check_time, in_vo_id, in_recalculationNeeded, in_maxAgeOfTupleInHours);
        end if;
    else
        insert into metricdata_spool_rejected(metricstatus_id, poem_sync_metric_id, service_id, fqan_id, vo_id, summarydata, gatheredat_id, check_time, insert_time)
            VALUES (in_metricstatus_id, in_poem_metric_id, in_service_id, in_fqan_id, in_vo_id, in_summarydata, in_gatheredat_id, in_check_time, in_insert_time);
        call logger('insertRowToMetricData', 'WARNING', 'Metric not in MRS bootstrapper --> REJECTED (metric_id ' || in_poem_metric_id || ' service_id ' || in_service_id || ' fqan_id ' || ifnull(in_fqan_id, -1) || ' vo_id '  || ifnull(in_vo_id, -1) || ')');
    end if;

  END IF;

END //

delimiter ;
