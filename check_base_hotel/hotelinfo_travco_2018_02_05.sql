-- MySQL dump 10.13  Distrib 5.1.73, for redhat-linux-gnu (x86_64)
--
-- Host: 10.10.228.253    Database: hotel_api
-- ------------------------------------------------------
-- Server version	5.7.16.k1-ucloudrel1-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `hotelinfo_travco_2018_02_05`
--

DROP TABLE IF EXISTS `hotelinfo_travco_2018_02_05`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `hotelinfo_travco_2018_02_05` (
  `hotel_name` varchar(512) DEFAULT NULL,
  `hotel_name_en` varchar(512) DEFAULT NULL,
  `source` varchar(64) NOT NULL,
  `source_id` varchar(128) NOT NULL,
  `brand_name` varchar(512) DEFAULT NULL,
  `map_info` varchar(512) DEFAULT NULL,
  `address` varchar(512) DEFAULT NULL,
  `city` varchar(512) DEFAULT NULL,
  `country` varchar(512) DEFAULT NULL,
  `city_id` varchar(11) NOT NULL,
  `postal_code` varchar(512) DEFAULT NULL,
  `star` varchar(20) DEFAULT NULL,
  `grade` varchar(256) DEFAULT NULL,
  `review_num` varchar(20) DEFAULT NULL,
  `has_wifi` varchar(20) DEFAULT NULL,
  `is_wifi_free` varchar(20) DEFAULT NULL,
  `has_parking` varchar(20) DEFAULT NULL,
  `is_parking_free` varchar(20) DEFAULT NULL,
  `service` text,
  `img_items` text,
  `description` text,
  `accepted_cards` varchar(512) DEFAULT NULL,
  `check_in_time` varchar(128) DEFAULT NULL,
  `check_out_time` varchar(128) DEFAULT NULL,
  `hotel_url` varchar(1024) DEFAULT NULL,
  `continent` varchar(96) DEFAULT NULL,
  `update_time` timestamp NULL DEFAULT NULL,
  `country_id` varchar(11) NOT NULL,
  PRIMARY KEY (`source`,`source_id`,`city_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hotelinfo_travco_2018_02_05`
--

LOCK TABLES `hotelinfo_travco_2018_02_05` WRITE;
/*!40000 ALTER TABLE `hotelinfo_travco_2018_02_05` DISABLE KEYS */;
INSERT INTO `hotelinfo_travco_2018_02_05` VALUES (NULL,'Generator (Testing)','travcoApi','YYG',NULL,'-0.159280,51.493603','11 Cadogan Gardens, Sloane Square','London','United Kingdom','10009',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'203'),(NULL,'Generator (Testing)','travcoApi','YYG1',NULL,'-0.178475,51.494553','130 Queen\'s Gate','London','United Kingdom','10009',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Generator (Testing)','travcoApi','YYG2',NULL,'-0.137000,51.498501','51 Buckingham Gate','London','United Kingdom','10009',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'203'),(NULL,'Generator (Testing)','travcoApi','YYG3',NULL,'-0.077399,51.525558','100 Shoreditch High Street','London','United Kingdom','10009',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','12:00',NULL,NULL,NULL,'203'),(NULL,'County (Testing)','travcoApi','YYL',NULL,'-0.172720,51.514938','159-161 Sussex Gardens','London','United Kingdom','10009',NULL,'2',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','10:30',NULL,NULL,NULL,'203'),(NULL,'Wedgewood (Testing)','travcoApi','YYW',NULL,'-0.124910,51.508453','Strand, Charing Cross','London','United Kingdom','10009',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','12:00',NULL,NULL,NULL,'203'),(NULL,'An-nur (Testing)','travcoApi','YYA',NULL,'-0.156533,51.514351','Bryanston Street','London','United Kingdom','10009',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'203'),(NULL,'Garden View (Testing)','travcoApi','TYY',NULL,'-0.188288,51.493958','16 Collingham Road','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','12:00',NULL,NULL,NULL,'203'),(NULL,'Westbury (Testing)','travcoApi','WYY',NULL,'-0.160790,51.524799','172 Gloucester Place','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Ambassadors (Testing)','travcoApi','YYM',NULL,'-0.081200,51.517300','40 Liverpool Street','London','United Kingdom','10009',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'203'),(NULL,'Ambassadors (Testing)','travcoApi','YY02',NULL,'-0.184937,51.511230','64 Queensborough Terrace','London','United Kingdom','10009',NULL,'2',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Ambassadors (Testing)','travcoApi','YYM1',NULL,'-0.173493,51.515530','13-17 Norfolk Square','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Ambassadors (Testing)','travcoApi','YYM2',NULL,'-0.143130,51.519791','20 Hallam Street','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Pembridge Palace (Testing)','travcoApi','YYP',NULL,'-0.184700,51.501701','60 Hyde Park Gate','London','United Kingdom','10009',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'203'),(NULL,'Brunel (Testing)','travcoApi','YYB',NULL,'-0.190398,51.492504','1 Barkston Gardens','London','United Kingdom','10009',NULL,'2',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Brunel (Testing)','travcoApi','1YB',NULL,'-0.192090,51.512821','8-16 Princes Square','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','12:00',NULL,NULL,NULL,'203'),(NULL,'Best Western The Cromwell (Testing)','travcoApi','YYS',NULL,'-0.123450,51.520690','83-93 Southampton Row','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'','',NULL,NULL,NULL,'203'),(NULL,'Kensington Court (Testing)','travcoApi','KYY',NULL,'-0.080572,51.497589','Bermonsey Square, Tower Bridge Road','London','United Kingdom','10009',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'','11:00',NULL,NULL,NULL,'203'),(NULL,'Kensington Court (Testing)','travcoApi','KYYA',NULL,'-0.191040,51.492241','18-26 Barkston Gardens','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Gainsborough (Testing)','travcoApi','YY01',NULL,'-0.178500,51.512001','12 Lancaster Gate','London','United Kingdom','10009',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'203'),(NULL,'Gainsborough (Testing)','travcoApi','YY03',NULL,'0.010900,51.603260','30 Oak Hill, Woodford Green','London','United Kingdom','10009',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'','',NULL,NULL,NULL,'203'),(NULL,'Montmartrois (Testing)','travcoApi','MZZ',NULL,'2.321050,48.890099','121 Avenue de Clichy','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Damremont (Testing)','travcoApi','ZZD',NULL,'2.365870,48.863899','13 Boulevard du Temple','Paris','France','10001',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','12:00',NULL,NULL,NULL,'201'),(NULL,'Victoria (Testing)','travcoApi','ZZV',NULL,'2.317530,48.890541','176 Rue Cardinet','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'201'),(NULL,'Europe Liege (Testing)','travcoApi','ZZE',NULL,'2.343671,48.873299','4 Rue Geoffroy-Marie','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'201'),(NULL,'Crowne Plaza Paris Republique (Testing)','travcoApi','ZZH',NULL,'2.227409,48.914972','2 rue Pierre Expert, Colombes','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Crowne Plaza Paris Republique (Testing)','travcoApi','ZZH1',NULL,'2.249030,48.894970','1 rue de Bitche, Courbevoie','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Crowne Plaza Paris Republique (Testing)','travcoApi','ZZH2',NULL,'2.234690,48.896870','88 Rue des Etudiants, Courbevoie','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Crowne Plaza Paris Republique (Testing)','travcoApi','ZZH3',NULL,'2.244210,48.884920','70 Rue Roque de Fillol, Puteaux','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Crowne Plaza Paris Republique (Testing)','travcoApi','ZZH4',NULL,'2.288727,48.915805','16 Rue des Freres Chausson, Asnieres-sur-Seine','Paris','France','10001',NULL,'2',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Le Lotti (Testing)','travcoApi','ZZL',NULL,'2.303971,48.898345','19-23 Avenue Anatole, Clichy','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Central Paris 4*+ Near Louvre (Testing)','travcoApi','3ZZ',NULL,'2.258244,48.726905','2A Place de L\'Union Europeenne, Massy','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Victoria Palace (Testing)','travcoApi','VZZ',NULL,'2.292831,48.821260','26 rue Jean Bleuzen, Vanves','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Victoria (Testing)','travcoApi','VZZZ',NULL,'2.252862,48.893860','2-4 Place Des Pleaides, Courbevoie','Paris','France','10001',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Victoria (Testing)','travcoApi','VZZY',NULL,'2.247962,48.888214','35 Court Michelet, Courbevoie','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Annexe (Testing)','travcoApi','UFY',NULL,'2.248849,48.894474','73 Avenue Gambetta, Courbevoie','Paris','France','10001',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'Beauchamps (Testing)','travcoApi','ZZ01',NULL,'2.785640,48.855919','42 Cours du Danube, Serris, Marne La Vallee','Paris','France','10001',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'201'),(NULL,'City Guest House (Testing)','travcoApi','QQC',NULL,'12.431722,41.875767','Via Giorgio Zoega 59','Rome','Italy','10002',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'205'),(NULL,'New York (Testing)','travcoApi','QQN',NULL,'12.232061,41.772000','Via Degli Orti 14, Fiumicino','Rome','Italy','10002',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'','',NULL,NULL,NULL,'205'),(NULL,'Amalfi (Testing)','travcoApi','QQA',NULL,'12.483800,41.901798','Piazza Accademia San Luca 74','Rome','Italy','10002',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'205'),(NULL,'Amalfi (Testing)','travcoApi','QQA1',NULL,'12.436660,41.919762','Via Damiano Chiesa 8','Rome','Italy','10002',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','11:00',NULL,NULL,NULL,'205'),(NULL,'Amalfi (Testing)','travcoApi','QQ4',NULL,'12.462300,41.904499','Via G.Vitelleschi 25','Rome','Italy','10002',NULL,'2',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'12:00','11:00',NULL,NULL,NULL,'205'),(NULL,'Donatello (Testing)','travcoApi','QQD',NULL,'12.503002,41.890388','Via Merulana 117','Rome','Italy','10002',NULL,'1',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'12:00','11:00',NULL,NULL,NULL,'205'),(NULL,'Doria (Testing)','travcoApi','DQQ',NULL,'12.590638,41.865765','Viale di Torre Maura 81','Rome','Italy','10002',NULL,'4',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','10:30',NULL,NULL,NULL,'205'),(NULL,'Flower Garden (Testing)','travcoApi','QQF',NULL,'12.485100,41.918774','Via Ulisse Aldrovandi 15','Rome','Italy','10002',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','12:00',NULL,NULL,NULL,'205'),(NULL,'Giglio del Opera (Testing)','travcoApi','QQG',NULL,'12.489907,41.905277','Via di San Basilio 15','Rome','Italy','10002',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'15:00','12:00',NULL,NULL,NULL,'205'),(NULL,'Santa Costanza (Testing)','travcoApi','QQS',NULL,'12.487900,41.904766','Via Vittorio Veneto 18','Rome','Italy','10002',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','12:00',NULL,NULL,NULL,'205'),(NULL,'Impero (Testing)','travcoApi','QQI',NULL,'12.499300,41.896400','Via Merulana 278','Rome','Italy','10002',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'205'),(NULL,'Caprice (Testing)','travcoApi','CQQ',NULL,'12.489700,41.906101','Via Vittorio Veneto 62','Rome','Italy','10002',NULL,'5',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'205'),(NULL,'Caprice (Testing)','travcoApi','CQQA',NULL,'12.502271,41.905220','Via Castelfidardo 55','Rome','Italy','10002',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:00','11:00',NULL,NULL,NULL,'205'),(NULL,'Caprice (Testing)','travcoApi','CQQB',NULL,'12.530790,41.885258','Via di Villa Serventi 9','Rome','Italy','10002',NULL,'3',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'14:30','11:00',NULL,NULL,NULL,'205');
/*!40000 ALTER TABLE `hotelinfo_travco_2018_02_05` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-02-05 17:50:08
