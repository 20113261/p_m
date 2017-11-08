# 融合前清表语句
DELETE FROM already_merged_city
WHERE type IN ('attr', 'attr_data');

DELETE FROM unknown_keywords
WHERE type LIKE '%attr%';

TRUNCATE attr_unid;

DELETE
FROM filter_data_already_online
WHERE type = 'attr';

# 融合后删除线上融合的数据
## 获取对应关系
SELECT
  id        AS new_id,
  source_id AS old_id
FROM attr_unid
WHERE source = 'online' AND id IN (SELECT id
                                   FROM attr_unid
                                   WHERE source = 'online'
                                   GROUP BY id
                                   HAVING count(*) > 1)
HAVING new_id != old_id
ORDER BY new_id;

## 删除更变的旧 online 景点
SELECT *
FROM chat_attraction
WHERE id IN (SELECT old_id
             FROM
               (SELECT
                  id        AS new_id,
                  source_id AS old_id
                FROM attr_unid
                WHERE source = 'online' AND id IN (SELECT id
                                                   FROM attr_unid
                                                   WHERE source = 'online'
                                                   GROUP BY id
                                                   HAVING count(*) > 1)
                HAVING new_id != old_id
                ORDER BY new_id) t);

DELETE FROM chat_attraction
WHERE id IN (SELECT old_id
             FROM
               (SELECT
                  id        AS new_id,
                  source_id AS old_id
                FROM attr_unid
                WHERE source = 'online' AND id IN (SELECT id
                                                   FROM attr_unid
                                                   WHERE source = 'online'
                                                   GROUP BY id
                                                   HAVING count(*) > 1)
                HAVING new_id != old_id
                ORDER BY new_id) t);
