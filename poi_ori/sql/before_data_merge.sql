# 融合前修改数据
UPDATE chat_attraction
SET alias = '',
  name    = '哈桑二世清真寺',
  name_en = 'Hassan II Mosque',
  url     = '{"daodao": "http://www.tripadvisor.cn/Attraction_Review-g293732-d317997-Reviews-Hassan_II_Mosque-Casablanca_Grand_Casablanca_Region.html"}'
WHERE id = 'v226840';

UPDATE chat_attraction
SET name  = '列宁墓',
  name_en = 'Lenin''s Mausoleum',
  alias   = '',
  url     = '{"daodao": "http://www.tripadvisor.cn/Attraction_Review-g298484-d301574-Reviews-Lenin_s_Mausoleum-Moscow_Central_Russia.html"}'
WHERE id = 'v226520';

UPDATE chat_attraction
SET name  = '查理金公园',
  name_en = 'Царицино',
  alias   = '',
  url     = '{"qyer": "http://place.qyer.com/poi/V2cJYFFuBzJTZlI3/"}'
WHERE id = 'v507093';

UPDATE chat_attraction
SET name  = '蓝洞',
  name_en = 'Blue Grotto',
  alias   = '',
  url     = '{"daodao": "http://www.tripadvisor.cn/Attraction_Review-g488299-d195537-Reviews-Blue_Grotto-Anacapri_Island_of_Capri_Province_of_Naples_Campania.html"}'
WHERE id = 'v224291';

# 关门好久的景点
DELETE FROM chat_attraction
WHERE id = 'v224519';

# 融合后修改数据
# id 会变
# UPDATE chat_attraction
# SET name = 'Museum of King Jan III''s Palace at Wilanow'
# WHERE id = 'v656289';

UPDATE chat_attraction
SET status_test = 'Open', status_online = 'Open'
WHERE id = 'v204110';

UPDATE chat_attraction
SET fix_ranking = 200
WHERE id in ('v501802', 'v513571');

# 药剂师之家
UPDATE chat_attraction
SET status_test = 'Close', status_online = 'Close'
WHERE id = 'v249111';

# 融合后数据修改
UPDATE chat_attraction
SET status_test = 'Close', status_online = 'Close'
WHERE id IN ('v233619', 'v510099', 'v285442', 'v249663');

SELECT *
FROM chat_attraction
WHERE id IN ('v233619', 'v510099', 'v285442', 'v249663');