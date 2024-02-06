DECLARE pending_transition_records INT64;
DECLARE retailer_names ARRAY<STRING>;
DECLARE counter INT64 DEFAULT 1;
DECLARE ide STRING DEFAULT "";

SET retailer_names = ['eBay', 'poshmark', 'TheRealReal', 'Etsy', 'Grailed', 'VestiaireCollective', 'Sellier', 'Rebag', 'BOPF', 'TheLuxuryCloset', 'Farfetch', 'HardlyEverWornIt'];

FOR retailer_name IN (SELECT value FROM UNNEST(retailer_names) as value)
DO

  SET pending_transition_records = (
    SELECT COUNT(*)
    FROM `phia-373523.inventory_dev.product_transition`
    WHERE transition_status = 'PENDING' AND secondhand_retailer = retailer_name.value
  );

  IF pending_transition_records > 0 THEN
    SELECT CONCAT('Did not merge feed for ', retailer_name.value, ' into product_transition table as records with pending status were found!') AS Message;
  ELSE
    MERGE `phia-373523.inventory_dev.product` t USING (SELECT DISTINCT * FROM `phia-373523.inventory_dev.product_transition` WHERE secondhand_retailer = retailer_name.value) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET unified_gender = s.unified_gender, unified_category = s.unified_category, secondhand_retailer = s.secondhand_retailer, age = s.age, firsthand_retailer = s.firsthand_retailer, displaysize = s.displaysize, secondhand_price = s.secondhand_price, condition = s.condition, filtersize = s.filtersize, categoryspecific = s.categoryspecific, color = s.color, secondhand_img_url = s.secondhand_img_url, other = s.other, categorybroad = s.categorybroad, gender = s.gender, id = s.id, secondhand_url = s.secondhand_url, unified_color = s.unified_color, timestamp = s.timestamp, NAME = s.NAME, secondhand_id = s.secondhand_id, secondhand_description = s.secondhand_description, img_url = s.img_url, size = s.size, material = s.material, category = s.category, last_seen = s.last_seen, last_modified = s.last_modified, transition_status = s.transition_status, transition_type = s.transition_type, in_vector_db = s.in_vector_db WHEN NOT MATCHED THEN INSERT ( unified_gender, unified_category, secondhand_retailer, age, firsthand_retailer, displaysize, secondhand_price, condition, filtersize, categoryspecific, color, secondhand_img_url, other, categorybroad, gender, id, secondhand_url, unified_color, NAME, secondhand_id, secondhand_description, img_url, size, material, category, last_seen, last_modified, transition_type, transition_status, in_vector_db ) VALUES ( unified_gender, unified_category, secondhand_retailer, age, firsthand_retailer, displaysize, secondhand_price, condition, filtersize, categoryspecific, color, secondhand_img_url, other, categorybroad, gender, id, secondhand_url, unified_color, NAME, secondhand_id, secondhand_description, img_url, size, material, category, last_seen, last_modified, transition_type, transition_status, in_vector_db );

    DELETE FROM `phia-373523.inventory_dev.product_transition` WHERE secondhand_retailer = retailer_name.value;

    CREATE OR REPLACE TEMP TABLE `product_helper` AS 
    SELECT * FROM `phia-373523.inventory_dev.product` WHERE secondhand_retailer = retailer_name.value;

    MERGE `product_helper` t USING (SELECT DISTINCT * FROM `phia-373523.inventory_dev.daily_feed` WHERE secondhand_retailer = retailer_name.value) s ON t.id = s.id WHEN MATCHED AND t.secondhand_price <> s.secondhand_price THEN UPDATE SET unified_gender = s.unified_gender, unified_category = s.unified_category, secondhand_retailer = s.secondhand_retailer, age = s.age, firsthand_retailer = s.firsthand_retailer, displaysize = s.displaysize, secondhand_price = s.secondhand_price, condition = s.condition, filtersize = s.filtersize, categoryspecific = s.categoryspecific, color = s.color, secondhand_img_url = s.secondhand_img_url, other = s.other, categorybroad = s.categorybroad, gender = s.gender, id = s.id, secondhand_url = s.secondhand_url, unified_color = s.unified_color, timestamp = s.timestamp, NAME = s.NAME, secondhand_id = s.secondhand_id, secondhand_description = s.secondhand_description, img_url = s.img_url, size = s.size, material = s.material, category = s.category, last_seen = s.last_seen, last_modified = s.last_modified, transition_status = 'PENDING', transition_type = CASE WHEN t.in_vector_db = true THEN 'UPDATE' ELSE 'INSERT' END WHEN MATCHED AND t.secondhand_price = s.secondhand_price THEN UPDATE SET last_seen = s.last_seen, transition_status = CASE WHEN t.in_vector_db = true THEN 'SUCCEEDED' ELSE 'PENDING' END, transition_type = CASE WHEN t.in_vector_db = true THEN 'INVARIANT' ELSE 'INSERT' END WHEN NOT MATCHED THEN INSERT ( unified_gender, unified_category, secondhand_retailer, age, firsthand_retailer, displaysize, secondhand_price, condition, filtersize, categoryspecific, color, secondhand_img_url, other, categorybroad, gender, id, secondhand_url, unified_color, NAME, secondhand_id, secondhand_description, img_url, size, material, category, last_seen, last_modified, transition_type, transition_status, in_vector_db ) VALUES ( unified_gender, unified_category, secondhand_retailer, age, firsthand_retailer, displaysize, secondhand_price, condition, filtersize, categoryspecific, color, secondhand_img_url, other, categorybroad, gender, id, secondhand_url, unified_color, NAME, secondhand_id, secondhand_description, img_url, size, material, category, last_seen, last_modified, 'INSERT', 'PENDING', false );

    INSERT INTO `phia-373523.inventory_dev.product_transition`
    SELECT * FROM `product_helper`; 
    
    IF retailer_name.value = 'eBay' THEN
        SET counter = 1; 
        LOOP
          IF NOT EXISTS(SELECT id FROM`phia-373523.inventory_dev.DummyTest` WHERE partition_index IS NULL AND secondhand_retailer='eBay' ORDER BY id ASC LIMIT 1) THEN 
             LEAVE;
          END IF;
          SET ide = (SELECT id FROM`phia-373523.inventory_dev.DummyTest` WHERE partition_index IS NULL AND secondhand_retailer='eBay' ORDER BY id ASC LIMIT 1);
          UPDATE `phia-373523.inventory_dev.DummyTest` SET partition_index = counter WHERE id = ide;
          SET counter=counter+1;
        END LOOP;
    ELSEIF retailer_name.value = 'poshmark' THEN
        SET counter = 45000001;
        LOOP
          IF NOT EXISTS(SELECT id FROM`phia-373523.inventory_dev.DummyTest` WHERE partition_index IS NULL AND secondhand_retailer='poshmark' ORDER BY id ASC LIMIT 1) THEN 
             LEAVE;
          END IF;
          SET ide = (SELECT id FROM`phia-373523.inventory_dev.DummyTest` WHERE partition_index IS NULL AND secondhand_retailer='poshmark' ORDER BY id ASC LIMIT 1);
          UPDATE `phia-373523.inventory_dev.DummyTest` SET partition_index = counter WHERE id = ide;
          SET counter=counter+1;
        END LOOP;
    ELSE
        SET counter = 135000001;
        LOOP
          IF NOT EXISTS(SELECT id FROM`phia-373523.inventory_dev.DummyTest` WHERE partition_index IS NULL AND secondhand_retailer!='eBay' AND secondhand_retailer!='poshmark' ORDER BY id ASC LIMIT 1) THEN 
             LEAVE;
          END IF;
          SET ide = (SELECT id FROM`phia-373523.inventory_dev.DummyTest` WHERE partition_index IS NULL AND secondhand_retailer!='eBay' AND secondhand_retailer!='poshmark' ORDER BY id ASC LIMIT 1);
          UPDATE `phia-373523.inventory_dev.DummyTest` SET partition_index = counter WHERE id = ide;
          SET counter=counter+1;
        END LOOP;
    END IF;
  
    DELETE FROM `phia-373523.inventory_dev.daily_feed` WHERE secondhand_retailer = retailer_name.value;
    DROP TABLE IF EXISTS `product_helper`;

  END IF;

END FOR;
