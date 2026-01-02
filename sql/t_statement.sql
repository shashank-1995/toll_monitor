-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Generation Time: Dec 10, 2025 at 10:47 AM
-- Server version: 9.1.0
-- PHP Version: 8.3.14

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `tst`
--

-- --------------------------------------------------------

--
-- Table structure for table `t_statement`
--

DROP TABLE IF EXISTS `t_statement`;
CREATE TABLE IF NOT EXISTS `t_statement` (
  `id` int NOT NULL AUTO_INCREMENT,
  `txn_dtm` datetime NOT NULL,
  `lic_no` varchar(50) NOT NULL,
  `tag_no` varchar(200) NOT NULL,
  `plaza_code` int NOT NULL,
  `plaza_name` varchar(60) NOT NULL,
  `rrn` varchar(100) NOT NULL,
  `trip_no` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `deduct_price` int NOT NULL,
  `created_at` varchar(250) NOT NULL,
  `status` varchar(250) NOT NULL,
  `state` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'na',
  `lst` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'na',
  `class` varchar(50) DEFAULT NULL,
  `gvw` int DEFAULT NULL,
  `axle_class` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `a` (`rrn`),
  KEY `c` (`plaza_code`),
  KEY `d` (`lic_no`,`txn_dtm`,`plaza_code`),
  KEY `e` (`lic_no`,`plaza_code`,`txn_dtm`),
  KEY `b` (`state`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `t_statement`
--
ALTER TABLE `t_statement` ADD FULLTEXT KEY `trip_no` (`trip_no`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
