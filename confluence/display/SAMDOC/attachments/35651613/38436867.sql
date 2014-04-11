DELIMITER //

DROP PROCEDURE IF EXISTS delete_obsolete_profiles;

CREATE PROCEDURE delete_obsolete_profiles (num_records INT)
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE counter INT;
  DECLARE tuple INT;
  DECLARE cur CURSOR FOR 
    select id from statuschange_service_profile where profile_id in (
      select id from profile where name in (
        'ARC'));

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
  SET counter = 0;
  OPEN cur;

  START TRANSACTION;
  cursor_loop: LOOP
    FETCH cur INTO tuple;
    IF done THEN
      LEAVE cursor_loop;
    END IF;

    delete from statuschange_service_profile where id = tuple;      
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
