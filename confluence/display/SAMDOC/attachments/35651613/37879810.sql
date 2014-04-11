DELIMITER //

DROP PROCEDURE IF EXISTS delete_obsolete_metrics;

CREATE PROCEDURE delete_obsolete_metrics (num_records INT)
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE counter INT;
  DECLARE tuple INT;
  DECLARE cur CURSOR FOR 
      select id from metricdata where metric_id in (
         select id from metric where name in (
             'org.sam.WN-CAver',
             'hr.srce.SRM1-CertLifetime',
             'org.gstat.CE',
             'org.gstat.SE',
             'org.gstat.Site',
             'org.nagios.LocalLogger-PortCheck',
             'org.nmap.CREAM-CE',
             'org.nmap.Central-LFC',
             'org.nmap.Classic-SE',
             'org.nmap.GRAM',
             'org.nmap.LB',
             'org.nmap.Local-LFC',
             'org.nmap.MyProxy',
             'org.nmap.SRM',
             'org.nmap.SRM1',
             'org.nmap.Site-BDII',
             'org.nmap.Top-BDII',
             'org.nmap.WMProxy',
             'org.nmap.WMS',
             'org.sam.CREAMCE-JobState',
             'org.osg.general.vo-supported',
             'org.osg.certificates.crl-expiry',
             'org.osg.batch.jobmanager-default-status',
             'org.osg.batch.jobmanagers-available',
             'org.osg.general.vdt-version',
             'hr.srce.VOMS-ServiceStatus',
             'org.sam.CREAMCE-DirectJobState',
             'org.sam.CE-JobState',
             'org.sam.mpi.CE-JobState',
             'ch.cern.LFC-Readdir',
             'ch.cern.LFC-ReadDli',
             'org.sam.CE-JobState',
             'hr.srce.CAdist-Version',
             'ch.cern.LFC-Readdir',
             'hr.srce.VOMS-ServiceStatus',
             'ch.cern.LFC-ReadDli',
             'ch.cern.RGMA-ServiceStatus',
             'org.nmap.MON',
             'org.ggus.Tickets',
             'org.nmap.FTS',
             'org.sam.CREAMCE-JobState',
             'org.sam.CREAMCE-DirectJobState',
             'org.arc.RLS',
             'org.osg.certificates.crl-expiry',
             'org.osg.batch.jobmanager-default-status',
             'org.osg.general.vo-supported',
             'org.osg.batch.jobmanagers-available',
             'org.osg.general.vdt-version',
             'org.osg.gums.authorization-status',
             'org.sam.mpi.CE-JobState',
             'org.lhcb.WN-lhcb-FileAccess',
             'hr.srce.RGMA-CertLifetime'));

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
  SET counter = 0;
  OPEN cur;

  START TRANSACTION;
  cursor_loop: LOOP
    FETCH cur INTO tuple;
    IF done THEN
      LEAVE cursor_loop;
    END IF;

	delete from metricdata where id = tuple;      
    SET counter = counter + 1;

    IF counter=num_records THEN
      COMMIT;
      START TRANSACTION;
      SET counter = 0;
    END IF;

  END LOOP cursor_loop;
  COMMIT;
END//

DELIMITER ;
