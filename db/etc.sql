CREATE TABLE `public_data_files` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type` varchar(45) NOT NULL,
  `user` int(11) DEFAULT NULL,
  `memo` varchar(45) NOT NULL,
  `path` varchar(256) NOT NULL,
  `size` int(11) DEFAULT 0,
  `auto_yn` char(1) DEFAULT 'N',
  `update_start_date` datetime DEFAULT NULL,
  `update_end_date` datetime DEFAULT NULL,
  `update_status` char(1) DEFAULT 'P' COMMENT 'P : 대기중 \\nR : 업데이트중 \\nY : 성공 \\nF : 실패   ',
  `update_count` int(11) DEFAULT NULL,
  `delete_yn` char(1) DEFAULT 'N',
  `cancel_yn` char(1) DEFAULT 'N',
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_type` (`type`),
  KEY `idx_cancel_yn` (`cancel_yn`),
  KEY `idx_create_date` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;


CREATE TABLE `road_width` (
  `id` int(10) unsigned NOT NULL,
  `road_name` varchar(45) DEFAULT NULL,
  `road_type` varchar(45) DEFAULT NULL,
  `road_func` varchar(45) DEFAULT NULL,
  `road_scale` varchar(45) DEFAULT NULL,
  `road_width` varchar(45) DEFAULT NULL,
  `road_owner` varchar(45) DEFAULT NULL,
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_name` (`road_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;