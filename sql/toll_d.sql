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
-- Table structure for table `toll_d`
--

DROP TABLE IF EXISTS `toll_d`;
CREATE TABLE IF NOT EXISTS `toll_d` (
  `id` int NOT NULL AUTO_INCREMENT,
  `type` varchar(250) NOT NULL,
  `subtype` varchar(250) NOT NULL,
  `priority` varchar(250) NOT NULL,
  `severity` varchar(250) NOT NULL,
  `tripno` varchar(250) NOT NULL,
  `amount` int NOT NULL,
  `title` varchar(250) NOT NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `code` int NOT NULL,
  `plaza_name` varchar(250) NOT NULL,
  `lic_no` varchar(250) NOT NULL,
  `dtm` varchar(250) DEFAULT NULL,
  `j_plaza` int DEFAULT NULL,
  `entry_plaza` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `a` (`tripno`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
