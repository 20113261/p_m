UPDATE country set status='Close';
UPDATE country SET status='Open' WHERE mid in  (SELECT DISTINCT country_id
             FROM city
             WHERE newProduct_status = 'Open');