-- --------------------------------------
-- PROCEDURE: VO_TOPOLOGY_UPDATE
-- --------------------------------------
DELIMITER $$

DROP PROCEDURE IF EXISTS `VO_TOPOLOGY_UPDATE`$$
CREATE PROCEDURE `VO_TOPOLOGY_UPDATE`(IN a_infrast_name VARCHAR(256),
  IN a_voname VARCHAR(100), IN a_atp_site_name VARCHAR(100), IN a_groupname VARCHAR(100), IN
  a_typename VARCHAR(100), IN a_hostname VARCHAR(256), IN
  a_service_flavour VARCHAR(50), IN a_spacetoken_name VARCHAR(512), IN
a_spacetoken_path VARCHAR(512),INOUT sucess_flag TINYINT)
BEGIN

  DECLARE v_groupsId         INTEGER;
  DECLARE v_grouptypeId      INTEGER;   
  DECLARE v_siteId           INTEGER DEFAULT -1;  
  DECLARE v_infrastId        INTEGER;
  DECLARE v_isDeleted        VARCHAR(1);
  DECLARE v_voId             INTEGER DEFAULT -1;
  DECLARE v_serviceId        INTEGER DEFAULT -1;
  DECLARE v_serviceflavourId INTEGER;
  DECLARE v_spacetokenId     INTEGER DEFAULT -1; 
  DECLARE v_updateTime       TIMESTAMP;
  DECLARE v_synchronizerId   INTEGER DEFAULT -1;

  DECLARE no_more_rows BOOLEAN DEFAULT FALSE;
  DECLARE sitecur CURSOR FOR SELECT id, infrast_id FROM site WHERE lower(sitename)=lower(a_atp_site_name);
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_more_rows = TRUE;

  SET sucess_flag=0;
  
  OPEN sitecur;

  sites_loop: LOOP
    SET v_siteId=-1;	
    FETCH sitecur INTO v_siteId,v_infrastId;

    IF no_more_rows THEN
      CLOSE sitecur;
      LEAVE sites_loop;
    END IF;

    -- check if groupname and group type name is null string
    IF a_typename='' OR a_groupname='' THEN
      CLOSE sitecur;
      LEAVE sites_loop;
    END IF;
	IF v_siteId =-1 THEN
	      SET sucess_flag=4;
	      CLOSE sitecur;
	      LEAVE sites_loop;
	END IF;
    SET v_voId = get_vo_id(a_voname);

    
    SET v_serviceflavourId = get_service_flavour_id(a_service_flavour);
    
    IF v_serviceflavourId =-1 THEN
      SET sucess_flag=2;
      CLOSE sitecur;
      LEAVE sites_loop;
    END IF; 
   
    SET v_serviceId = get_service_id(a_hostname,v_serviceflavourId);

    IF v_serviceId =-1 THEN
      SET sucess_flag=3;
      CLOSE sitecur;
      LEAVE sites_loop;
    END IF; 


    SET v_updateTime = CURRENT_TIMESTAMP;
    
    SET v_synchronizerId = get_synchronizer_id('vo-feeds');
    
    SET v_grouptypeId = get_group_type_id(a_typename);
    -- add group type
    IF v_grouptypeId =-1 THEN
     
          INSERT INTO group_type(typename,description)
          VALUES(a_typename, concat(a_voname,':',a_typename));
	  SET v_grouptypeId=LAST_INSERT_ID();
    ELSE
	UPDATE group_type SET isdeleted='N' WHERE id=v_grouptypeId;	
    END IF;

   
    SET v_groupsId = get_group_id(a_groupname, a_typename);
    -- add group
    IF v_groupsId =-1 THEN
     
          INSERT INTO groups(group_type_id,groupname,description)
          VALUES(v_grouptypeId,a_groupname, concat(a_voname,':',a_groupname));
    ELSE
	UPDATE groups SET isdeleted='N' WHERE id=v_groupsId;	
    END IF;

  
    IF ((v_voId != -1) AND (v_siteId != -1) AND (v_groupsId > -1) AND (v_serviceId !=-1) AND (v_synchronizerId !=-1)) THEN
      BEGIN
        DECLARE EXIT HANDLER FOR NOT FOUND
        BEGIN
          INSERT INTO vo_service_group(vo_id, service_id, groups_id)
          VALUES(v_voId, v_serviceId, v_groupsId);
        END;

        SELECT isdeleted INTO v_isDeleted
        FROM vo_service_group
        WHERE service_id=v_serviceId
          AND vo_id=v_voId
          AND groups_id=v_groupsId;

        IF (v_isDeleted = 'Y') THEN
          UPDATE vo_service_group
          SET isdeleted='N'
          WHERE service_id=v_serviceId
            AND groups_id=v_groupsId
            AND vo_id=v_voId;
        END IF;
      END;
      IF ASCII(a_spacetoken_name)>0 THEN
      -- insert or update spacetoken and spacetoken last seen
      	BEGIN
        	DECLARE EXIT HANDLER FOR NOT FOUND
        	BEGIN
          	-- spacetoken
          	INSERT INTO space_token(service_id,tokenname,tokenpath)
          	VALUES(v_serviceId,a_spacetoken_name,a_spacetoken_path);
          
          	SET v_spacetokenId=LAST_INSERT_ID();   
          
          	-- spacetoken lastseen
          	INSERT INTO stoken_last_seen(space_token_id,synchronizer_id,lastseen)
          	VALUES (v_spacetokenId,v_synchronizerId,v_updateTime);
        	
		END;
        	
		SELECT id,isdeleted INTO v_spacetokenId,v_isDeleted
        	FROM space_token
        	WHERE service_id=v_serviceId
        	AND tokenname=a_spacetoken_name;
        	IF (v_isDeleted = 'Y') THEN
           		UPDATE space_token 
           		SET isdeleted='N'
           		WHERE service_id=v_serviceId
           		AND tokenname=a_spacetoken_name;
        	END IF;
        	-- update last seen time stamp
        	UPDATE stoken_last_seen
        	SET lastseen=v_updateTime 
        	WHERE space_token_id=v_spacetokenId
        	AND synchronizer_id=v_synchronizerId;  
      	END;
      -- insert or update vo spacetoken group
      	BEGIN
        	DECLARE EXIT HANDLER FOR NOT FOUND
        	BEGIN
         	-- vo spacetoken group
        		INSERT INTO vo_stoken_group(vo_id,space_token_id,groups_id)
        		VALUES(v_voId,v_spacetokenId,v_groupsId);
        	END;

        	SELECT isdeleted INTO v_isDeleted
        	FROM vo_stoken_group
        	WHERE space_token_id=v_spacetokenId
          		AND vo_id=v_voId
          		AND groups_id=v_groupsId;

        	IF (v_isDeleted = 'Y') THEN
          		UPDATE vo_stoken_group
          		SET isdeleted='N'
          		WHERE space_token_id=v_spacetokenId
            		AND groups_id=v_groupsId
            		AND vo_id=v_voId;
        	END IF;
      	END; 
       END IF;	
      -- insert or update vo-group
      BEGIN
        DECLARE EXIT HANDLER FOR NOT FOUND
        BEGIN
          INSERT INTO vo_group(vo_id,groups_id)
          VALUES(v_voId,v_groupsId);
        END;
        
        SELECT isdeleted INTO v_isDeleted
        FROM vo_group
        WHERE vo_id=v_voId AND groups_id=v_groupsId;
        
        IF (v_isDeleted = 'Y') THEN
            UPDATE vo_group
              SET isdeleted='N'
            WHERE groups_id=v_groupsId
              AND vo_id=v_voId;
        END IF;
      END;      
      SET sucess_flag=1;
    END IF;
 
   SET no_more_rows = FALSE;
  END LOOP sites_loop;
END$$

DELIMITER ;

